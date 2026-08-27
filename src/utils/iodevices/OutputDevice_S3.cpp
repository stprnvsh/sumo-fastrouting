/****************************************************************************/
// Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.dev/sumo
// Copyright (C) 2026 German Aerospace Center (DLR) and others.
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0/
// This Source Code may also be made available under the following Secondary
// Licenses when the conditions for such availability set forth in the Eclipse
// Public License 2.0 are satisfied: GNU General Public License, version 2
// or later which is available at
// https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html
// SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later
/****************************************************************************/
/// @file    OutputDevice_S3.cpp
/// @author  Pranav Sateesh
/// @date    2026-08-27
///
// An output device that uploads to S3 compatible object storage
/****************************************************************************/
#include <config.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <streambuf>
#include <thread>
#include <vector>
#ifdef HAVE_CURL
#include <curl/curl.h>
#endif
#include <utils/common/MsgHandler.h>
#include <utils/common/StringUtils.h>
#include <utils/common/ToString.h>
#include <utils/common/UtilExceptions.h>
#include "OutputDevice_S3.h"


// ===========================================================================
// class OutputDevice_S3::Uploader
// ===========================================================================
#ifdef HAVE_CURL

/**
 * @class OutputDevice_S3::Uploader
 * @brief Stream buffer collecting output into fixed size parts and uploading them
 *
 * Everything written to the stream lands in a part buffer. Small outputs are
 *  uploaded with a single PutObject on finalize; once the buffer fills up a
 *  multipart upload is started and each full buffer becomes one part, so the
 *  memory use never exceeds one part regardless of the output size.
 */
class OutputDevice_S3::Uploader : public std::streambuf {
public:
    Uploader(const std::string& url) {
        // s3://bucket/key
        const std::string::size_type keyPos = url.find('/', 5);
        if (keyPos == std::string::npos || keyPos == 5 || keyPos + 1 == url.size()) {
            throw IOError(TLF("Invalid S3 output '%', expected s3://bucket/key.", url));
        }
        myBucket = url.substr(5, keyPos - 5);
        myKey = url.substr(keyPos + 1);
        myEnvCredentials.accessKey = getEnv("AWS_ACCESS_KEY_ID");
        myEnvCredentials.secretKey = getEnv("AWS_SECRET_ACCESS_KEY");
        myEnvCredentials.token = getEnv("AWS_SESSION_TOKEN");
        // the endpoint serving rotating role credentials in ECS / EKS containers
        myCredentialsURI = getEnv("AWS_CONTAINER_CREDENTIALS_FULL_URI");
        if (myCredentialsURI == "") {
            const std::string relative = getEnv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
            if (relative != "") {
                myCredentialsURI = "http://169.254.170.2" + relative;
            }
        }
        myRegion = getEnv("AWS_REGION");
        if (myRegion == "") {
            myRegion = getEnv("AWS_DEFAULT_REGION");
        }
        if (myRegion == "") {
            myRegion = "us-east-1";
        }
        if (myEnvCredentials.accessKey == "" && myCredentialsURI == "") {
            throw IOError(TLF("S3 output '%' needs credentials in AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or a container credentials endpoint (AWS_CONTAINER_CREDENTIALS_FULL_URI).", url));
        }
        const std::string endpoint = getEnv("AWS_ENDPOINT_URL");
        if (endpoint != "") {
            // custom endpoints (local object stores) are addressed path-style
            myBucketURL = endpoint;
            while (!myBucketURL.empty() && myBucketURL.back() == '/') {
                myBucketURL.pop_back();
            }
            myBucketURL += "/" + myBucket;
        } else {
            myBucketURL = "https://" + myBucket + ".s3." + myRegion + ".amazonaws.com";
        }
        myBaseURL = myBucketURL + "/" + encodeKey(myKey);
        static std::once_flag initFlag;
        std::call_once(initFlag, []() {
            curl_global_init(CURL_GLOBAL_DEFAULT);
        });
        myCurl = curl_easy_init();
        if (myCurl == nullptr) {
            throw IOError(TL("Could not initialize libcurl."));
        }
        // check the endpoint, credentials and bucket now, so misconfigured runs
        // stop right away instead of losing their output at the final upload
        std::string err;
        if (!request("HEAD", "", nullptr, 0, nullptr, err, nullptr, myBucketURL.c_str())) {
            throw IOError(TLF("The bucket of the output '%' is not accessible (%).", url, err));
        }
        myBuffer.resize(PART_SIZE);
        setp(myBuffer.data(), myBuffer.data() + myBuffer.size());
    }

    ~Uploader() {
        if (myCurl != nullptr) {
            curl_easy_cleanup(myCurl);
        }
    }

    /// @brief whether an upload request has failed (the object will not be created)
    bool broken() const {
        return myBroken;
    }

    /// @brief what went wrong when an upload request has failed
    const std::string& error() const {
        return myError;
    }

    /// @brief whether the object has been (or failed to be) written out completely
    bool finalized() const {
        return myFinalized;
    }

    /** @brief Uploads all remaining buffered output and makes the object visible
     * @exception IOError If any request failed (a started multipart upload is aborted then)
     */
    void finalize() {
        if (myBroken) {
            throw IOError(TLF("The upload to '%' failed (%).", "s3://" + myBucket + "/" + myKey, myError));
        }
        if (myFinalized) {
            return;
        }
        myFinalized = true;
        std::string err;
        if (myUploadId == "") {
            // everything fit into the buffer, create the object with a single request
            if (!request("PUT", "", pbase(), pptr() - pbase(), nullptr, err)) {
                fail(err);
            }
        } else {
            if ((pptr() != pbase() && !uploadPart(err)) || !completeUpload(err)) {
                fail(err);
            }
        }
        setp(myBuffer.data(), myBuffer.data() + myBuffer.size());
    }

protected:
    /// @name Methods that override/implement std::streambuf-methods
    /// @{

    /// @brief uploads the full part buffer and stores the given character in the freed buffer
    int_type overflow(int_type c) override {
        if (myBroken) {
            return traits_type::eof();
        }
        std::string err;
        if (!ensureUpload(err) || !uploadPart(err)) {
            // exceptions cannot pass through the ostream, the failure is
            // rethrown by the next postWriteHook or by finalize
            myFinalized = true;
            fail(err, false);
            return traits_type::eof();
        }
        if (c != traits_type::eof()) {
            *pptr() = traits_type::to_char_type(c);
            pbump(1);
        }
        return traits_type::not_eof(c);
    }

    /// @brief parts become visible only on finalize, so flushing the stream is a no-op
    int sync() override {
        return myBroken ? -1 : 0;
    }

    /// @brief reports the total number of bytes written (needed for tellp, e.g. by the Parquet writer)
    pos_type seekoff(off_type off, std::ios_base::seekdir way, std::ios_base::openmode which) override {
        if (off == 0 && way == std::ios_base::cur && (which & std::ios_base::out) != 0) {
            return pos_type(off_type(myUploadedSize + (pptr() - pbase())));
        }
        return pos_type(off_type(-1));
    }
    /// @}

private:
    /// @brief parts need at least 5MiB (except the last), larger parts mean fewer requests
    static const size_t PART_SIZE = 16 * 1024 * 1024;

    /// @brief S3 rejects part numbers beyond 10000
    static const int MAX_PARTS = 10000;

    /// @brief returns the value of the given environment variable or "" if it is not set
    static std::string getEnv(const char* var) {
        const char* const val = getenv(var);
        return val == nullptr ? "" : val;
    }

    /// @brief percent-encodes everything in the object key except unreserved characters and '/'
    static std::string encodeKey(const std::string& key) {
        static const char* const hex = "0123456789ABCDEF";
        std::string result;
        for (const char c : key) {
            if (isalnum((unsigned char)c) || c == '-' || c == '.' || c == '_' || c == '~' || c == '/') {
                result += c;
            } else {
                result += '%';
                result += hex[(unsigned char)c >> 4];
                result += hex[c & 0xF];
            }
        }
        return result;
    }

    /// @brief records the failure and aborts a started multipart upload, optionally throwing as IOError
    void fail(const std::string& err, const bool doThrow = true) {
        myBroken = true;
        myError = err;
        abortUpload();
        if (doThrow) {
            throw IOError(TLF("The upload to '%' failed (%).", "s3://" + myBucket + "/" + myKey, err));
        }
    }

    /// @brief starts the multipart upload if it is not running yet
    bool ensureUpload(std::string& err) {
        if (myUploadId != "") {
            return true;
        }
        std::string response;
        if (!request("POST", "uploads=", "", 0, &response, err)) {
            return false;
        }
        const std::string::size_type start = response.find("<UploadId>");
        const std::string::size_type end = response.find("</UploadId>");
        if (start == std::string::npos || end == std::string::npos) {
            err = "no upload id in '" + response.substr(0, 200) + "'";
            return false;
        }
        myUploadId = response.substr(start + 10, end - start - 10);
        return true;
    }

    /// @brief uploads the current buffer content as the next part and resets the buffer
    bool uploadPart(std::string& err) {
        if ((int)myETags.size() == MAX_PARTS) {
            err = "the output exceeds the maximum upload size of " + toString(MAX_PARTS * PART_SIZE / (1024 * 1024)) + "MB";
            return false;
        }
        char* encodedId = curl_easy_escape(myCurl, myUploadId.c_str(), (int)myUploadId.size());
        const std::string query = "partNumber=" + toString(myETags.size() + 1) + "&uploadId=" + encodedId;
        curl_free(encodedId);
        std::string etag;
        if (!request("PUT", query, pbase(), pptr() - pbase(), nullptr, err, &etag)) {
            return false;
        }
        if (etag == "") {
            err = "no ETag in the part upload response";
            return false;
        }
        myETags.push_back(etag);
        myUploadedSize += pptr() - pbase();
        setp(myBuffer.data(), myBuffer.data() + myBuffer.size());
        return true;
    }

    /// @brief completes the multipart upload from the collected part ETags
    bool completeUpload(std::string& err) {
        std::string body = "<CompleteMultipartUpload>";
        for (int i = 0; i < (int)myETags.size(); i++) {
            body += "<Part><PartNumber>" + toString(i + 1) + "</PartNumber><ETag>" + myETags[i] + "</ETag></Part>";
        }
        body += "</CompleteMultipartUpload>";
        char* encodedId = curl_easy_escape(myCurl, myUploadId.c_str(), (int)myUploadId.size());
        const std::string query = "uploadId=" + std::string(encodedId);
        curl_free(encodedId);
        std::string response;
        if (!request("POST", query, body.data(), body.size(), &response, err)) {
            return false;
        }
        // completion errors come wrapped in a 200 response
        if (response.find("<Error>") != std::string::npos) {
            err = "completing failed with '" + response.substr(0, 200) + "'";
            return false;
        }
        myUploadId = "";
        return true;
    }

    /// @brief abandons a started multipart upload so the parts do not linger (best effort)
    void abortUpload() {
        if (myUploadId != "") {
            char* encodedId = curl_easy_escape(myCurl, myUploadId.c_str(), (int)myUploadId.size());
            std::string err;
            request("DELETE", "uploadId=" + std::string(encodedId), nullptr, 0, nullptr, err);
            curl_free(encodedId);
            myUploadId = "";
        }
    }

    static size_t writeCallback(char* data, size_t size, size_t n, void* into) {
        static_cast<std::string*>(into)->append(data, size * n);
        return size * n;
    }

    /** @brief Performs one signed S3 request, retrying transient failures
     * @param[in] method The HTTP method to use
     * @param[in] query The (canonically ordered) query string, "" for none
     * @param[in] data The request body (nullptr for an empty body)
     * @param[in] size The request body size
     * @param[out] response If given, receives the response body
     * @param[out] err Receives a description of the failure
     * @param[out] etag If given, receives the ETag response header
     * @param[in] urlOverride A URL to address instead of the object (bucket probing)
     * @return whether the request succeeded (HTTP 2xx)
     */
    bool request(const char* method, const std::string& query, const char* data, const size_t size,
                 std::string* response, std::string& err, std::string* etag = nullptr, const char* urlOverride = nullptr) {
        Credentials credentials;
        if (!getCredentials(credentials, err)) {
            return false;
        }
        const bool head = strcmp(method, "HEAD") == 0;
        const std::string url = urlOverride != nullptr ? std::string(urlOverride)
                                : (query == "" ? myBaseURL : myBaseURL + "?" + query);
        const std::string sigv4 = "aws:amz:" + myRegion + ":s3";
        const std::string userpwd = credentials.accessKey + ":" + credentials.secretKey;
        for (int attempt = 0;; attempt++) {
            curl_easy_reset(myCurl);
            curl_slist* headers = curl_slist_append(nullptr, "Content-Type: application/octet-stream");
            headers = curl_slist_append(headers, "Expect:");
            if (credentials.token != "") {
                headers = curl_slist_append(headers, ("x-amz-security-token: " + credentials.token).c_str());
            }
            std::string body;
            std::string responseHeaders;
            char curlErr[CURL_ERROR_SIZE] = "";
            curl_easy_setopt(myCurl, CURLOPT_URL, url.c_str());
            if (head) {
                curl_easy_setopt(myCurl, CURLOPT_NOBODY, 1L);
            } else {
                curl_easy_setopt(myCurl, CURLOPT_CUSTOMREQUEST, method);
                curl_easy_setopt(myCurl, CURLOPT_POSTFIELDS, data == nullptr ? "" : data);
                curl_easy_setopt(myCurl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)size);
            }
            curl_easy_setopt(myCurl, CURLOPT_AWS_SIGV4, sigv4.c_str());
            curl_easy_setopt(myCurl, CURLOPT_USERPWD, userpwd.c_str());
            curl_easy_setopt(myCurl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(myCurl, CURLOPT_WRITEFUNCTION, writeCallback);
            curl_easy_setopt(myCurl, CURLOPT_WRITEDATA, &body);
            curl_easy_setopt(myCurl, CURLOPT_HEADERFUNCTION, writeCallback);
            curl_easy_setopt(myCurl, CURLOPT_HEADERDATA, &responseHeaders);
            curl_easy_setopt(myCurl, CURLOPT_ERRORBUFFER, curlErr);
            curl_easy_setopt(myCurl, CURLOPT_NOSIGNAL, 1L);
            curl_easy_setopt(myCurl, CURLOPT_CONNECTTIMEOUT, 10L);
            curl_easy_setopt(myCurl, CURLOPT_LOW_SPEED_LIMIT, 1L);
            curl_easy_setopt(myCurl, CURLOPT_LOW_SPEED_TIME, 120L);
            const CURLcode result = curl_easy_perform(myCurl);
            long status = 0;
            curl_easy_getinfo(myCurl, CURLINFO_RESPONSE_CODE, &status);
            curl_slist_free_all(headers);
            if (result == CURLE_OK && status >= 200 && status < 300) {
                if (response != nullptr) {
                    *response = body;
                }
                if (etag != nullptr) {
                    *etag = findETag(responseHeaders);
                }
                return true;
            }
            if (result != CURLE_OK) {
                err = curlErr[0] == '\0' ? curl_easy_strerror(result) : curlErr;
            } else {
                err = "HTTP " + toString(status) + ": " + body.substr(0, 200);
            }
            const bool transient = result != CURLE_OK || status >= 500 || status == 429;
            if (!transient || attempt == 2) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1 << attempt));
        }
    }

    /// @brief extracts the ETag value from the response headers
    static std::string findETag(const std::string& headers) {
        std::istringstream stream(headers);
        std::string line;
        while (std::getline(stream, line)) {
            if (StringUtils::to_lower_case(line.substr(0, 5)) == "etag:") {
                return StringUtils::prune(line.substr(5));
            }
        }
        return "";
    }

    /// @brief a set of (possibly temporary) credentials to sign requests with
    struct Credentials {
        std::string accessKey, secretKey, token;
    };

    /** @brief Provides the credentials to sign the next request with
     *
     * With a container credentials endpoint configured the role credentials
     *  are fetched from there and refreshed periodically, so rotation during
     *  long runs is picked up. Otherwise the environment credentials are used.
     */
    bool getCredentials(Credentials& into, std::string& err) {
        if (myCredentialsURI == "") {
            into = myEnvCredentials;
            return true;
        }
        const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        if (!myHaveRoleCredentials || now - myCredentialsFetched > std::chrono::minutes(5)) {
            std::string body;
            if (!plainGet(myCredentialsURI, body, err)) {
                err = "fetching role credentials failed (" + err + ")";
                return false;
            }
            myRoleCredentials.accessKey = findJsonString(body, "AccessKeyId");
            myRoleCredentials.secretKey = findJsonString(body, "SecretAccessKey");
            myRoleCredentials.token = findJsonString(body, "Token");
            if (myRoleCredentials.accessKey == "" || myRoleCredentials.secretKey == "") {
                err = "no credentials in the response of '" + myCredentialsURI + "'";
                return false;
            }
            myHaveRoleCredentials = true;
            myCredentialsFetched = now;
        }
        into = myRoleCredentials;
        return true;
    }

    /// @brief performs an unsigned GET against the credentials endpoint
    bool plainGet(const std::string& url, std::string& body, std::string& err) {
        curl_easy_reset(myCurl);
        curl_slist* headers = nullptr;
        std::string token = getEnv("AWS_CONTAINER_AUTHORIZATION_TOKEN");
        const std::string tokenFile = getEnv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE");
        if (tokenFile != "") {
            // EKS pod identity rotates the token file, read it fresh
            std::ifstream stream(tokenFile);
            std::stringstream content;
            content << stream.rdbuf();
            token = StringUtils::prune(content.str());
        }
        if (token != "") {
            headers = curl_slist_append(headers, ("Authorization: " + token).c_str());
        }
        char curlErr[CURL_ERROR_SIZE] = "";
        curl_easy_setopt(myCurl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(myCurl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(myCurl, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(myCurl, CURLOPT_WRITEDATA, &body);
        curl_easy_setopt(myCurl, CURLOPT_ERRORBUFFER, curlErr);
        curl_easy_setopt(myCurl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(myCurl, CURLOPT_CONNECTTIMEOUT, 10L);
        curl_easy_setopt(myCurl, CURLOPT_TIMEOUT, 30L);
        const CURLcode result = curl_easy_perform(myCurl);
        long status = 0;
        curl_easy_getinfo(myCurl, CURLINFO_RESPONSE_CODE, &status);
        curl_slist_free_all(headers);
        if (result != CURLE_OK) {
            err = curlErr[0] == '\0' ? curl_easy_strerror(result) : curlErr;
            return false;
        }
        if (status < 200 || status >= 300) {
            err = "HTTP " + toString(status);
            return false;
        }
        return true;
    }

    /// @brief extracts a flat string field from a JSON object
    static std::string findJsonString(const std::string& json, const std::string& name) {
        std::string::size_type pos = json.find("\"" + name + "\"");
        if (pos == std::string::npos) {
            return "";
        }
        pos = json.find('"', json.find(':', pos));
        if (pos == std::string::npos) {
            return "";
        }
        const std::string::size_type end = json.find('"', pos + 1);
        return end == std::string::npos ? "" : json.substr(pos + 1, end - pos - 1);
    }

private:
    /// @brief bucket, key and the region to sign for
    std::string myBucket, myKey, myRegion;

    /// @brief the URL of the bucket (for probing it on construction)
    std::string myBucketURL;

    /// @brief what went wrong when an upload request has failed
    std::string myError;

    /// @brief the credentials taken from the environment
    Credentials myEnvCredentials;

    /// @brief the URL serving rotating role credentials ("" when using environment credentials)
    std::string myCredentialsURI;

    /// @brief the last credentials fetched from the endpoint and their age
    Credentials myRoleCredentials;
    bool myHaveRoleCredentials = false;
    std::chrono::steady_clock::time_point myCredentialsFetched;

    /// @brief the URL of the object without query parameters
    std::string myBaseURL;

    /// @brief the reused curl handle
    CURL* myCurl = nullptr;

    /// @brief the part buffer serving as the streambuf put area
    std::vector<char> myBuffer;

    /// @brief the id of the running multipart upload ("" as long as everything fits into the buffer)
    std::string myUploadId;

    /// @brief the ETags of the uploaded parts, needed for completing the upload
    std::vector<std::string> myETags;

    /// @brief the number of bytes already uploaded (excluding the buffer content)
    long long int myUploadedSize = 0;

    /// @brief whether an upload request has failed
    bool myBroken = false;

    /// @brief whether finalize has run
    bool myFinalized = false;
};

#else  // HAVE_CURL

class OutputDevice_S3::Uploader : public std::streambuf {
public:
    bool broken() const {
        return true;
    }
    bool finalized() const {
        return true;
    }
    void finalize() {}
    const std::string& error() const {
        return myError;
    }
private:
    std::string myError;
};

#endif  // HAVE_CURL


// ===========================================================================
// method definitions
// ===========================================================================
OutputDevice_S3::OutputDevice_S3(const std::string& fullName)
    : OutputDevice(0, fullName) {
#ifdef HAVE_CURL
    myUploader = new Uploader(fullName);
    myOStream = new std::ostream(myUploader);
#else
    throw IOError(TLF("Cannot write to '%', SUMO was compiled without libcurl.", fullName));
#endif
}


OutputDevice_S3::~OutputDevice_S3() {
    // we need to cleanup the formatter first, because it still might have cached data
    delete myFormatter;
    myFormatter = nullptr;
    if (myUploader != nullptr && !myUploader->finalized()) {
        // close() was skipped, upload what we have but never throw from here
        try {
            myOStream->flush();
            myUploader->finalize();
        } catch (const IOError& e) {
            std::cerr << e.what() << std::endl;
        }
    }
    delete myOStream;
    delete myUploader;
}


bool
OutputDevice_S3::ok() {
    return myOStream->good() && !myUploader->broken();
}


void
OutputDevice_S3::postWriteHook() {
    if (myUploader->broken()) {
        // surface upload failures which could not pass through the ostream,
        // in the same way a broken socket connection is reported
        throw IOError(TLF("The upload to '%' failed (%).", getFilename(), myUploader->error()));
    }
}


void
OutputDevice_S3::onClose() {
    // the formatter may hold cached data (parquet row groups and footer), it
    // has to reach the stream before the last part is uploaded
    delete myFormatter;
    myFormatter = nullptr;
    myOStream->flush();
    myUploader->finalize();
}
