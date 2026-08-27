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
/// @file    OutputDevice_S3.h
/// @author  Pranav Sateesh
/// @date    2026-08-27
///
// An output device that uploads to S3 compatible object storage
/****************************************************************************/
#pragma once
#include <config.h>

#include <ostream>
#include <string>
#include "OutputDevice.h"


// ===========================================================================
// class definitions
// ===========================================================================
/**
 * @class OutputDevice_S3
 * @brief An output device that uploads to S3 compatible object storage
 *
 * Output written to "s3://bucket/key" is buffered in memory and uploaded
 *  with a single PutObject request on closing. Larger outputs are streamed
 *  as a multipart upload with fixed size parts so the memory use stays
 *  bounded independent of the output size.
 *
 * The device works with any store speaking the S3 protocol. Credentials and
 *  the region come from the environment variable names established across
 *  the S3 ecosystem (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
 *  AWS_SESSION_TOKEN, AWS_REGION). The endpoint of the store is given with
 *  AWS_ENDPOINT_URL and is addressed path-style; without it the AWS endpoint
 *  for the region is used and role credentials can be fetched from a
 *  container credentials endpoint (AWS_CONTAINER_CREDENTIALS_FULL_URI).
 */
class OutputDevice_S3 : public OutputDevice {
public:
    /// @brief whether the given output name addresses an S3 object
    static bool isS3(const std::string& name) {
        return name.compare(0, 5, "s3://") == 0;
    }

    /** @brief Constructor
     * @param[in] fullName The "s3://bucket/key" URL of the object to create
     * @exception IOError If the URL is malformed, credentials are missing or SUMO was compiled without libcurl
     */
    OutputDevice_S3(const std::string& fullName);

    /// @brief Destructor
    ~OutputDevice_S3();

    /** @brief returns the information whether one can write into the device
     * @return Whether the device can be used (no upload has failed)
     */
    bool ok() override;

protected:
    /// @name Methods that override/implement OutputDevice-methods
    /// @{

    /** @brief Returns the associated ostream
     * @return The used stream
     */
    inline std::ostream& getOStream() override {
        return *myOStream;
    }

    /** @brief Reports upload failures which could not pass through the ostream
     * @exception IOError If a part upload has failed
     */
    void postWriteHook() override;

    /** @brief Completes the upload when the device is closed
     * @exception IOError If completing the upload failed (the upload is aborted then)
     */
    void onClose() override;
    /// @}

private:
    /// @brief the upload state and part buffer (defined in the .cpp to keep libcurl out of the header)
    class Uploader;

    /// @brief The upload backend, also acting as stream buffer
    Uploader* myUploader = nullptr;

    /// @brief The stream writing into the upload buffer
    std::ostream* myOStream = nullptr;

};
