/****************************************************************************/
// Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.dev/sumo
// Copyright (C) 2012-2026 German Aerospace Center (DLR) and others.
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
/// @file    ParquetFormatter.cpp
/// @author  Michael Behrisch
/// @author  Pranav Sateesh
/// @date    2025-06-17
///
// An output formatter for Parquet files
/****************************************************************************/
#include <config.h>

#ifdef _MSC_VER
#pragma warning(push)
/* Disable warning about unused parameters */
#pragma warning(disable: 4100)
/* Disable warning about hidden function arrow::io::Writable::Write */
#pragma warning(disable: 4266)
/* Disable warning about padded memory layout */
#pragma warning(disable: 4324)
/* Disable warning about this in initializers */
#pragma warning(disable: 4355)
/* Disable warning about changed memory layout due to virtual base class */
#pragma warning(disable: 4435)
/* Disable warning about declaration hiding class member */
#pragma warning(disable: 4458)
#endif
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/thread_pool.h>
#include <parquet/arrow/writer.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

#include <utils/common/MsgHandler.h>
#include <utils/common/ToString.h>
#include "ParquetFormatter.h"


// ===========================================================================
// helper class definitions
// ===========================================================================
class ArrowOStreamWrapper : public arrow::io::OutputStream {
public:
    ArrowOStreamWrapper(std::ostream& out)
        : myOStream(out), myAmOpen(true) {}

    arrow::Status Close() override {
        myAmOpen = false;
        return arrow::Status::OK();
    }

    arrow::Status Flush() override {
        myOStream.flush();
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override {
        return myOStream.tellp();
    }

    bool closed() const override {
        return !myAmOpen;
    }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        if (!myAmOpen) {
            return arrow::Status::IOError("Write on closed stream");
        }
        myOStream.write(reinterpret_cast<const char*>(data), nbytes);
        if (!myOStream) {
            return arrow::Status::IOError("Failed to write to ostream");
        }
        return arrow::Status::OK();
    }

private:
    std::ostream& myOStream;
    bool myAmOpen;
};


// ===========================================================================
// ParquetFormatter::Impl definition
// ===========================================================================
struct ParquetFormatter::Impl {
    /// @brief the concrete builder type per column, for devirtualized appends
    enum class ColType : unsigned char { F64, F32, I32, STR };

    /// @brief a typed staging slot for one attribute value of the current row.
    /// Replaces the former shared_ptr<arrow::Scalar> boxing: no heap allocation
    /// per value (the string buffer is reused across rows because slots only
    /// ever get overwritten, never destroyed).
    struct StagedValue {
        enum class Kind : unsigned char { NULLV, F64, F32, I32, STR };
        Kind kind = Kind::NULLV;
        double d = 0.;
        float f = 0.f;
        int i = 0;
        std::string s;

        void setNull() {
            kind = Kind::NULLV;
        }
        void set(const double v) {
            kind = Kind::F64;
            d = v;
        }
        void set(const float v) {
            kind = Kind::F32;
            f = v;
        }
        void set(const int v) {
            kind = Kind::I32;
            i = v;
        }
        void set(const std::string& v) {
            kind = Kind::STR;
            s.assign(v);
        }
    };

    Impl(const std::string& columnNames, const int batchSize, const bool async, const int encodeThreads)
        : myHeaderFormat(columnNames), myBatchSize(batchSize), myAsync(async), myEncodeThreads(encodeThreads) {}

    /// @brief the format to use for the column names
    const std::string myHeaderFormat;

    /// @brief the compression to use
    parquet::Compression::type myCompression = parquet::Compression::UNCOMPRESSED;

    /// @brief the number of rows to write per batch
    const int myBatchSize;

    /// @brief whether batches are encoded and written by a background thread
    const bool myAsync;

    /// @brief the number of threads for encoding / compressing the columns of a row group
    const int myEncodeThreads;

    /// @brief the currently read tag (only valid when generating the header)
    std::string myCurrentTag;

    /// @brief the table schema
    std::shared_ptr<arrow::Schema> mySchema = arrow::schema({});

    /// @brief the output stream writer
    std::unique_ptr<parquet::arrow::FileWriter> myParquetWriter;

    /// @brief the content array builders for the table
    std::vector<std::shared_ptr<arrow::ArrayBuilder> > myBuilders;

    /// @brief the concrete type of each builder (parallel to myBuilders)
    std::vector<ColType> myColTypes;

    /// @brief The number of attributes in the currently open XML elements
    std::vector<int> myXMLStack;

    /// @brief physical staging slots for the current row (only ever grows)
    std::vector<StagedValue> myRow;

    /// @brief the logical number of staged values (see nextSlot / closeTag)
    int myRowSize = 0;

    /// @brief the maximum depth of the XML hierarchy
    int myMaxDepth = 2;

    /// @brief whether the schema has been constructed completely
    bool myWroteHeader = false;

    /// @brief whether this formatter only stages rows for another (writing) formatter
    bool myStagerMode = false;

    /// @brief the next staged row / value to be consumed by the writing formatter (stagers only)
    int myConsumeRow = 0;
    size_t myConsumeVal = 0;

    /// @brief whether the columns should be checked for completeness
    bool myCheckColumns = false;

    /// @brief whether there is still unwritten data
    bool myNeedsWrite = false;

    /// @brief whether any root attribute have been encountered
    bool myHaveRootAttrs = false;

    /// @brief the attributes which are expected for a complete row (including null values)
    SumoXMLAttrMask myExpectedAttrs;

    /// @brief the attributes already seen (including null values)
    SumoXMLAttrMask mySeenAttrs;

    /// @name background writer state (Level 2). The simulation thread only
    /// copies each finished row into a flat chunk (values + one string arena,
    /// memcpy-scale work); the writer thread performs the builder appends,
    /// Finish, encoding, compression and the stream write. A single ordered
    /// consumer with the same batch boundaries keeps the row groups in
    /// production order, so the file is byte-identical to a synchronous write.
    /// @{

    /// @brief a block of staged rows handed to the writer thread
    struct RowChunk {
        struct Val {
            StagedValue::Kind kind;
            double d;
            float f;
            int i;
            unsigned strOff;
            unsigned strLen;
        };
        std::vector<Val> vals;
        std::vector<int> rowLens;
        std::string arena;

        void clear() {
            vals.clear();
            rowLens.clear();
            arena.clear();
        }
        int rows() const {
            return (int)rowLens.size();
        }
    };

    /// @brief rows per chunk handed to the writer thread
    static const int CHUNK_ROWS = 65536;
    /// @brief maximum number of rows waiting for the writer thread. The bound
    /// is expressed in rows rather than chunks because chunk sizes vary widely:
    /// the serial path fills chunks up to CHUNK_ROWS while parallel row stagers
    /// (see enqueueStagedRows) hand over one small chunk per slice and step.
    /// Four full chunks match the historic worst case memory; a single chunk
    /// may exceed the bound by itself so that oversized chunks still make
    /// progress.
    static const int MAX_QUEUED_ROWS = 4 * CHUNK_ROWS;

    std::thread myWriterThread;
    std::mutex myMutex;
    std::condition_variable myCvProduce;
    std::condition_variable myCvConsume;
    std::deque<std::unique_ptr<RowChunk> > myQueue;
    /// @brief the number of rows currently in the queue (guarded by myMutex)
    int myQueuedRows = 0;
    /// @brief consumed chunks recycled back to the producer (arena capacity kept)
    std::vector<std::unique_ptr<RowChunk> > myFreeChunks;
    /// @brief the chunk currently being filled by the simulation thread
    std::unique_ptr<RowChunk> myCurChunk;
    /// @brief rows currently accumulated in the builders (writer thread only)
    int myRowsInBuilders = 0;
    bool myShutdown = false;
    /// @brief the first error encountered on the writer thread (sticky)
    std::string myAsyncError;
    /// @}

    /// @brief column-name lookup honoring the headerFormat option
    std::string getAttrString(const std::string& attrString) const {
        if (myHeaderFormat == "plain") {
            return attrString;
        }
        if (myHeaderFormat == "auto") {
            for (const auto& field : mySchema->fields()) {
                if (field->name() == attrString) {
                    return myCurrentTag + "_" + attrString;
                }
            }
            return attrString;
        }
        return myCurrentTag + "_" + attrString;
    }

    void checkAttr(const SumoXMLAttr attr) {
        if (myCheckColumns && myMaxDepth == (int)myXMLStack.size()) {
            mySeenAttrs.set(attr);
            if (!myExpectedAttrs.test(attr)) {
                throw ProcessError(TLF("Unexpected attribute '%', this file format does not support Parquet output yet.", toString(attr)));
            }
        }
    }

    /// @brief the staging slot for the next attribute of the current row
    StagedValue& nextSlot() {
        if (myRowSize == (int)myRow.size()) {
            myRow.emplace_back();
        }
        return myRow[myRowSize++];
    }

    template <class ATTR_TYPE, class BUILDER>
    void checkBuilder(const ATTR_TYPE& attr, const std::shared_ptr<arrow::DataType>& (*dataType)(), const ColType colType) {
        myNeedsWrite = true;
        if (!myWroteHeader) {
            const std::string fieldName = getAttrString(toString(attr));
            int idx = 0;
            for (const auto& field : mySchema->fields()) {
                if (field->name() == fieldName) {
                    // there might be missing attributes inbetween, so make sure the position of the attribute in the header matches the current number of values
                    while (idx > myRowSize) {
                        nextSlot().setNull();
                    }
                    return;
                }
                idx++;
            }
            mySchema = *mySchema->AddField(mySchema->num_fields(), arrow::field(fieldName, dataType()));
            auto builder = std::make_shared<BUILDER>();
            if (!myBuilders.empty()) {
                if (myBuilders.back()->length() > 0) {
                    PARQUET_THROW_NOT_OK(builder->AppendNulls(myBuilders.back()->length()));
                }
                while (myRowSize < (int)myBuilders.size()) {
                    nextSlot().setNull();
                }
            }
            myBuilders.push_back(builder);
            myColTypes.push_back(colType);
        }
    }

    /// @brief devirtualized append of a staged value into its column builder
    void appendTyped(const int col, const StagedValue& v) {
        arrow::ArrayBuilder* const builder = myBuilders[col].get();
        if (v.kind == StagedValue::Kind::NULLV) {
            PARQUET_THROW_NOT_OK(builder->AppendNull());
            return;
        }
        switch (myColTypes[col]) {
            case ColType::F64:
                switch (v.kind) {
                    case StagedValue::Kind::F64:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder*>(builder)->Append(v.d));
                        return;
                    case StagedValue::Kind::F32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder*>(builder)->Append((double)v.f));
                        return;
                    case StagedValue::Kind::I32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder*>(builder)->Append((double)v.i));
                        return;
                    default:
                        break;
                }
                break;
            case ColType::F32:
                switch (v.kind) {
                    case StagedValue::Kind::F32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder*>(builder)->Append(v.f));
                        return;
                    case StagedValue::Kind::F64:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder*>(builder)->Append((float)v.d));
                        return;
                    case StagedValue::Kind::I32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder*>(builder)->Append((float)v.i));
                        return;
                    default:
                        break;
                }
                break;
            case ColType::I32:
                if (v.kind == StagedValue::Kind::I32) {
                    PARQUET_THROW_NOT_OK(static_cast<arrow::Int32Builder*>(builder)->Append(v.i));
                    return;
                }
                break;
            case ColType::STR:
                if (v.kind == StagedValue::Kind::STR) {
                    PARQUET_THROW_NOT_OK(static_cast<arrow::StringBuilder*>(builder)->Append(v.s));
                    return;
                }
                break;
        }
        throw ProcessError(TLF("Mismatched value type for column '%'.", mySchema->field(col)->name()));
    }

    /// @brief copy the current staged row into the chunk being filled
    /// (simulation thread; memcpy-scale work only)
    void stageRowToChunk() {
        if (myCurChunk == nullptr) {
            myCurChunk = takeFreeChunk();
        }
        RowChunk& chunk = *myCurChunk;
        for (int i = 0; i < myRowSize; ++i) {
            const StagedValue& v = myRow[i];
            RowChunk::Val cv;
            cv.kind = v.kind;
            cv.d = v.d;
            cv.f = v.f;
            cv.i = v.i;
            cv.strOff = 0;
            cv.strLen = 0;
            if (v.kind == StagedValue::Kind::STR) {
                cv.strOff = (unsigned)chunk.arena.size();
                cv.strLen = (unsigned)v.s.size();
                chunk.arena.append(v.s);
            }
            chunk.vals.push_back(cv);
        }
        chunk.rowLens.push_back(myRowSize);
        if (chunk.rows() >= CHUNK_ROWS) {
            enqueueChunk();
        }
    }

    /// @brief hand the current chunk to the writer thread (bounded queue)
    void enqueueChunk() {
        if (myStagerMode || myCurChunk == nullptr || myCurChunk->rows() == 0) {
            // a stager keeps its rows until the writing formatter consumes them
            return;
        }
        pushChunk(std::move(myCurChunk));
    }

    /// @brief append a filled chunk to the writer queue (bounded, kept in call order)
    void pushChunk(std::unique_ptr<RowChunk> chunk) {
        std::unique_lock<std::mutex> lock(myMutex);
        if (!myWriterThread.joinable()) {
            myWriterThread = std::thread(&Impl::writerLoop, this);
        }
        myCvProduce.wait(lock, [this] {
            return myQueuedRows < MAX_QUEUED_ROWS || !myAsyncError.empty();
        });
        checkAsyncErrorLocked();
        myQueuedRows += chunk->rows();
        myQueue.push_back(std::move(chunk));
        myCvConsume.notify_one();
    }

    /// @brief a recycled (or fresh) chunk for the producer
    std::unique_ptr<RowChunk> takeFreeChunk() {
        std::lock_guard<std::mutex> lock(myMutex);
        if (!myFreeChunks.empty()) {
            std::unique_ptr<RowChunk> chunk = std::move(myFreeChunks.back());
            myFreeChunks.pop_back();
            return chunk;
        }
        return std::make_unique<RowChunk>();
    }

    /// @brief consume queued chunks in order (runs on the writer thread)
    void writerLoop() {
        while (true) {
            std::unique_ptr<RowChunk> chunk;
            {
                std::unique_lock<std::mutex> lock(myMutex);
                myCvConsume.wait(lock, [this] {
                    return !myQueue.empty() || myShutdown;
                });
                if (myQueue.empty()) {
                    break;  // shutdown and drained; flush the partial batch below
                }
                chunk = std::move(myQueue.front());
                myQueue.pop_front();
                myQueuedRows -= chunk->rows();
            }
            try {
                consumeChunk(*chunk);
            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> lock(myMutex);
                if (myAsyncError.empty()) {
                    myAsyncError = e.what();
                }
            }
            chunk->clear();
            {
                std::lock_guard<std::mutex> lock(myMutex);
                myFreeChunks.push_back(std::move(chunk));
            }
            myCvProduce.notify_all();
        }
        // end of document: write the final (partial) row group
        if (myRowsInBuilders > 0) {
            try {
                finishAndWriteBatch();
            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> lock(myMutex);
                if (myAsyncError.empty()) {
                    myAsyncError = e.what();
                }
            }
        }
    }

    /// @brief append the rows of a chunk into the builders, flushing complete
    /// batches at exactly the same boundaries as the synchronous path
    void consumeChunk(const RowChunk& chunk) {
        size_t valIdx = 0;
        for (const int rowLen : chunk.rowLens) {
            int index = 0;
            for (int col = 0; col < (int)myBuilders.size(); ++col) {
                if (index < rowLen) {
                    const RowChunk::Val& cv = chunk.vals[valIdx + index];
                    index++;
                    appendChunkVal(col, cv, chunk.arena);
                } else {
                    PARQUET_THROW_NOT_OK(myBuilders[col]->AppendNull());
                }
            }
            valIdx += rowLen;
            if (++myRowsInBuilders >= myBatchSize) {
                finishAndWriteBatch();
            }
        }
    }

    /// @brief devirtualized append of a chunk value into its column builder
    void appendChunkVal(const int col, const RowChunk::Val& cv, const std::string& arena) {
        arrow::ArrayBuilder* const builder = myBuilders[col].get();
        if (cv.kind == StagedValue::Kind::NULLV) {
            PARQUET_THROW_NOT_OK(builder->AppendNull());
            return;
        }
        switch (myColTypes[col]) {
            case ColType::F64:
                switch (cv.kind) {
                    case StagedValue::Kind::F64:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder*>(builder)->Append(cv.d));
                        return;
                    case StagedValue::Kind::F32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder*>(builder)->Append((double)cv.f));
                        return;
                    case StagedValue::Kind::I32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder*>(builder)->Append((double)cv.i));
                        return;
                    default:
                        break;
                }
                break;
            case ColType::F32:
                switch (cv.kind) {
                    case StagedValue::Kind::F32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder*>(builder)->Append(cv.f));
                        return;
                    case StagedValue::Kind::F64:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder*>(builder)->Append((float)cv.d));
                        return;
                    case StagedValue::Kind::I32:
                        PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder*>(builder)->Append((float)cv.i));
                        return;
                    default:
                        break;
                }
                break;
            case ColType::I32:
                if (cv.kind == StagedValue::Kind::I32) {
                    PARQUET_THROW_NOT_OK(static_cast<arrow::Int32Builder*>(builder)->Append(cv.i));
                    return;
                }
                break;
            case ColType::STR:
                if (cv.kind == StagedValue::Kind::STR) {
                    PARQUET_THROW_NOT_OK(static_cast<arrow::StringBuilder*>(builder)->Append(
                                             arena.data() + cv.strOff, (int)cv.strLen));
                    return;
                }
                break;
        }
        throw ProcessError(TLF("Mismatched value type for column '%'.", mySchema->field(col)->name()));
    }

    /// @brief Finish the builders and write one row group / record batch
    void finishAndWriteBatch() {
        std::vector<std::shared_ptr<arrow::Array> > data;
        for (auto& builder : myBuilders) {
            std::shared_ptr<arrow::Array> column;
            PARQUET_THROW_NOT_OK(builder->Finish(&column));
            data.push_back(column);
        }
        auto batch = arrow::RecordBatch::Make(mySchema, data.back()->length(), data);
        PARQUET_THROW_NOT_OK(myParquetWriter->WriteRecordBatch(*batch));
        myRowsInBuilders = 0;
    }

    /// @brief flush all pending rows and stop the writer thread
    /// (end of document / destruction)
    void drainAndJoin() {
        if (myStagerMode) {
            return;
        }
        enqueueChunk();  // the partial chunk, if any
        if (myWriterThread.joinable()) {
            {
                std::lock_guard<std::mutex> lock(myMutex);
                myShutdown = true;
            }
            myCvConsume.notify_all();
            myWriterThread.join();
            std::lock_guard<std::mutex> lock(myMutex);
            checkAsyncErrorLocked();
        }
    }

    /// @brief rethrow the first writer-thread error on the simulation thread
    void checkAsyncErrorLocked() {
        if (!myAsyncError.empty()) {
            const std::string msg = myAsyncError;
            myAsyncError.clear();
            throw ProcessError(TLF("Parquet writer thread failed: %", msg));
        }
    }
};


// ===========================================================================
// member method definitions
// ===========================================================================
ParquetFormatter::ParquetFormatter(const std::string& columnNames, const std::string& compression,
                                   const bool async, const int encodeThreads, const int batchSize)
    : OutputFormatter(OutputFormatterType::PARQUET), myImpl(std::make_unique<Impl>(columnNames, batchSize, async, encodeThreads)) {
    if (compression == "snappy") {
        myImpl->myCompression = parquet::Compression::SNAPPY;
    } else if (compression == "gzip") {
        myImpl->myCompression = parquet::Compression::GZIP;
    } else if (compression == "brotli") {
        myImpl->myCompression = parquet::Compression::BROTLI;
    } else if (compression == "zstd") {
        myImpl->myCompression = parquet::Compression::ZSTD;
    } else if (compression == "lz4") {
        myImpl->myCompression = parquet::Compression::LZ4;
    } else if (compression == "bz2") {
        myImpl->myCompression = parquet::Compression::BZ2;
    } else if (compression != "" && compression != "uncompressed") {
        WRITE_ERRORF("Unknown compression: %", compression);
    }
    if (!arrow::util::Codec::IsAvailable(myImpl->myCompression)) {
        WRITE_WARNINGF("Compression '%' not available, falling back to uncompressed.", compression);
        myImpl->myCompression = parquet::Compression::UNCOMPRESSED;
    }
}


ParquetFormatter::~ParquetFormatter() {
    if (myImpl != nullptr) {
        try {
            myImpl->drainAndJoin();
        } catch (const ProcessError& e) {
            WRITE_ERROR(e.what());
        }
    }
}


bool
ParquetFormatter::writeXMLHeader(std::ostream& into, const std::string& rootElement,
                                 const std::map<SumoXMLAttr, std::string>& attrs, bool /* writeMetadata */,
                                 bool /* includeConfig */) {
    if (attrs.size() > 2) {
        myImpl->myHaveRootAttrs = true;
        openTag(into, rootElement);
        for (const auto& a : attrs) {
            if (a.first != SUMO_ATTR_XMLNS && a.first != SUMO_ATTR_SCHEMA_LOCATION) {
                writeAttr(into, a.first, a.second, false);
            }
        }
        return true;
    }
    return false;
}


void
ParquetFormatter::openTag(std::ostream& /* into */, const std::string& xmlElement) {
    myImpl->myXMLStack.push_back(myImpl->myRowSize);
    if (!myImpl->myWroteHeader) {
        myImpl->myCurrentTag = xmlElement;
    }
    // stagers run on worker threads where the message handler may not be used
    if (myImpl->myMaxDepth == (int)myImpl->myXMLStack.size() && myImpl->myWroteHeader && !myImpl->myStagerMode && myImpl->myCurrentTag != xmlElement) {
        WRITE_WARNINGF("Encountered mismatch in XML tags (expected % but got %). Column names may be incorrect.", myImpl->myCurrentTag, xmlElement);
    }
}


void
ParquetFormatter::openTag(std::ostream& /* into */, const SumoXMLTag& xmlElement) {
    myImpl->myXMLStack.push_back(myImpl->myRowSize);
    if (!myImpl->myWroteHeader) {
        myImpl->myCurrentTag = toString(xmlElement);
    }
    if (myImpl->myMaxDepth == (int)myImpl->myXMLStack.size() && myImpl->myWroteHeader && !myImpl->myStagerMode && myImpl->myCurrentTag != toString(xmlElement)) {
        WRITE_WARNINGF("Encountered mismatch in XML tags (expected % but got %). Column names may be incorrect.", myImpl->myCurrentTag, toString(xmlElement));
    }
}


bool
ParquetFormatter::closeTag(std::ostream& into, const std::string& /* comment */) {
    if (myImpl->myMaxDepth == 0) {
        // the auto detection case: the first closed tag determines the depth
        myImpl->myMaxDepth = (int)myImpl->myXMLStack.size();
    }
    const bool eof = myImpl->myXMLStack.empty() || (myImpl->myHaveRootAttrs && myImpl->myXMLStack.size() == 1);
    if ((myImpl->myMaxDepth == (int)myImpl->myXMLStack.size() || eof) && !myImpl->myWroteHeader) {
        // we are at the correct depth or the document has ended (XML stack is empty)
        // so we should initialize the writer with the schema (if not done yet)
        if (!myImpl->myCheckColumns) {
            WRITE_WARNING("Column based formats are still experimental. Autodetection only works for homogeneous output.");
        }
        auto arrow_stream = std::make_shared<ArrowOStreamWrapper>(into);
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder().compression(myImpl->myCompression)->build();
        // Arrow encodes / compresses the columns of a row group in parallel on its
        // CPU thread pool (0 = default pool size, > 1 caps the pool); with 1 the
        // encoding stays on the writing thread and no pool threads are spawned.
        // The file bytes are identical either way.
        const bool encodeParallel = myImpl->myAsync && myImpl->myEncodeThreads != 1;
        if (encodeParallel && myImpl->myEncodeThreads > 1) {
            PARQUET_THROW_NOT_OK(arrow::SetCpuThreadPoolCapacity(myImpl->myEncodeThreads));
        }
        std::shared_ptr<parquet::ArrowWriterProperties> arrowProps =
            parquet::ArrowWriterProperties::Builder().set_use_threads(encodeParallel)->build();
        myImpl->myParquetWriter = *parquet::arrow::FileWriter::Open(*myImpl->mySchema, arrow::default_memory_pool(), arrow_stream, props, arrowProps);
        myImpl->myWroteHeader = true;
    }
    bool writeBatch = false;
    if (myImpl->myNeedsWrite) {
        // the attribute completeness of staged rows was already checked on the first (serially written) row
        if (myImpl->myCheckColumns && !myImpl->myStagerMode && (int)myImpl->myXMLStack.size() == myImpl->myMaxDepth && myImpl->myExpectedAttrs != myImpl->mySeenAttrs) {
            for (int i = 0; i < (int)myImpl->myExpectedAttrs.size(); ++i) {
                if (myImpl->myExpectedAttrs.test(i) && !myImpl->mySeenAttrs.test(i)) {
                    WRITE_ERRORF("Incomplete attribute set, '%' is missing. This file format does not support Parquet output yet.",
                                 toString((SumoXMLAttr)i));
                }
            }
        }
        if (myImpl->myAsync && myImpl->myWroteHeader) {
            // Level 2: copy the row into the current chunk; the writer thread
            // performs the builder appends, encoding, compression and writing.
            myImpl->stageRowToChunk();
        } else {
            // Level 1 (synchronous path): devirtualized typed appends from the
            // staged row; positions beyond the staged values become nulls.
            int index = 0;
            for (int col = 0; col < (int)myImpl->myBuilders.size(); ++col) {
                if (index < myImpl->myRowSize) {
                    myImpl->appendTyped(col, myImpl->myRow[index++]);
                } else {
                    PARQUET_THROW_NOT_OK(myImpl->myBuilders[col]->AppendNull());
                }
            }
            writeBatch = myImpl->myWroteHeader && myImpl->myBuilders.back()->length() >= myImpl->myBatchSize;
        }
        myImpl->mySeenAttrs.reset();
        myImpl->myNeedsWrite = false;
    }
    if (!myImpl->myAsync && (writeBatch || (eof && !myImpl->myBuilders.empty()))) {
        std::vector<std::shared_ptr<arrow::Array> > data;
        for (auto& builder : myImpl->myBuilders) {
            std::shared_ptr<arrow::Array> column;
            PARQUET_THROW_NOT_OK(builder->Finish(&column));
            data.push_back(column);
        }
        auto batch = arrow::RecordBatch::Make(myImpl->mySchema, data.back()->length(), data);
        PARQUET_THROW_NOT_OK(myImpl->myParquetWriter->WriteRecordBatch(*batch));
    }
    if (myImpl->myAsync && eof) {
        // flush pending rows; everything must be on disk before the writer
        // destructor emits the footer
        myImpl->drainAndJoin();
    }
    if (!myImpl->myXMLStack.empty()) {
        if (myImpl->myRowSize > myImpl->myXMLStack.back()) {
            myImpl->myRowSize = myImpl->myXMLStack.back();
        }
        myImpl->myXMLStack.pop_back();
    }
    return false;
}


void
ParquetFormatter::writeAttr(std::ostream& into, const SumoXMLAttr attr, const double& val, const bool isNull) {
    myImpl->checkAttr(attr);
    if (attr == SUMO_ATTR_X || attr == SUMO_ATTR_Y || into.precision() > 2) {
        myImpl->checkBuilder<SumoXMLAttr, arrow::DoubleBuilder>(attr, arrow::float64, Impl::ColType::F64);
        if (isNull) {
            myImpl->nextSlot().setNull();
        } else {
            myImpl->nextSlot().set(val);
        }
    } else {
        myImpl->checkBuilder<SumoXMLAttr, arrow::FloatBuilder>(attr, arrow::float32, Impl::ColType::F32);
        if (isNull) {
            myImpl->nextSlot().setNull();
        } else {
            myImpl->nextSlot().set((float)val);
        }
    }
}


void
ParquetFormatter::writeAttr(std::ostream& /* into */, const SumoXMLAttr attr, const int& val, const bool isNull) {
    myImpl->checkAttr(attr);
    myImpl->checkBuilder<SumoXMLAttr, arrow::Int32Builder>(attr, arrow::int32, Impl::ColType::I32);
    if (isNull) {
        myImpl->nextSlot().setNull();
    } else {
        myImpl->nextSlot().set(val);
    }
}


void
ParquetFormatter::writeAttr(std::ostream& into, const std::string& attr, const double& val, const bool isNull) {
    assert(!myImpl->myCheckColumns);
    if (into.precision() > 2) {
        myImpl->checkBuilder<std::string, arrow::DoubleBuilder>(attr, arrow::float64, Impl::ColType::F64);
        if (isNull) {
            myImpl->nextSlot().setNull();
        } else {
            myImpl->nextSlot().set(val);
        }
    } else {
        myImpl->checkBuilder<std::string, arrow::FloatBuilder>(attr, arrow::float32, Impl::ColType::F32);
        if (isNull) {
            myImpl->nextSlot().setNull();
        } else {
            myImpl->nextSlot().set((float)val);
        }
    }
}


void
ParquetFormatter::writeAttr(std::ostream& /* into */, const std::string& attr, const int& val, const bool isNull) {
    assert(!myImpl->myCheckColumns);
    myImpl->checkBuilder<std::string, arrow::Int32Builder>(attr, arrow::int32, Impl::ColType::I32);
    if (isNull) {
        myImpl->nextSlot().setNull();
    } else {
        myImpl->nextSlot().set(val);
    }
}


void
ParquetFormatter::writeStringAttr(const SumoXMLAttr attr, const std::string& val) {
    myImpl->checkAttr(attr);
    myImpl->checkBuilder<SumoXMLAttr, arrow::StringBuilder>(attr, arrow::utf8, Impl::ColType::STR);
    myImpl->nextSlot().set(val);
}


void
ParquetFormatter::writeStringAttr(const std::string& attr, const std::string& val) {
    assert(!myImpl->myCheckColumns);
    myImpl->checkBuilder<std::string, arrow::StringBuilder>(attr, arrow::utf8, Impl::ColType::STR);
    myImpl->nextSlot().set(val);
}


void
ParquetFormatter::writeNullAttr(const SumoXMLAttr attr) {
    myImpl->checkAttr(attr);
    myImpl->checkBuilder<SumoXMLAttr, arrow::StringBuilder>(attr, arrow::utf8, Impl::ColType::STR);
    myImpl->nextSlot().setNull();
}


void
ParquetFormatter::writeNullAttr(const std::string& attr) {
    assert(!myImpl->myCheckColumns);
    myImpl->checkBuilder<std::string, arrow::StringBuilder>(attr, arrow::utf8, Impl::ColType::STR);
    myImpl->nextSlot().setNull();
}


void
ParquetFormatter::writeTime(std::ostream& /* into */, const SumoXMLAttr attr, const SUMOTime val) {
    if (!gHumanReadableTime) {
        // always float64 for machine-readable time, regardless of stream precision
        myImpl->checkBuilder<SumoXMLAttr, arrow::DoubleBuilder>(attr, arrow::float64, Impl::ColType::F64);
        myImpl->nextSlot().set(STEPS2TIME(val));
        return;
    }
    writeStringAttr(attr, time2string(val));
}


bool
ParquetFormatter::wroteHeader() const {
    return myImpl->myWroteHeader;
}


void
ParquetFormatter::setExpectedAttributes(const SumoXMLAttrMask& expected, const int depth) {
    myImpl->myExpectedAttrs = expected;
    myImpl->myMaxDepth = depth;
    myImpl->myCheckColumns = expected.any();
}


bool
ParquetFormatter::supportsParallelRows() const {
    // the schema (and with it the column typing) must be frozen and the
    // asynchronous staging path must be active before rows may be staged elsewhere
    return myImpl->myAsync && myImpl->myWroteHeader && !myImpl->myStagerMode;
}


OutputFormatter*
ParquetFormatter::createRowStager() const {
    if (!supportsParallelRows()) {
        return nullptr;
    }
    ParquetFormatter* const stager = new ParquetFormatter(myImpl->myHeaderFormat, "", true, myImpl->myEncodeThreads, myImpl->myBatchSize);
    Impl& si = *stager->myImpl;
    si.myStagerMode = true;
    // share the frozen schema and typing so the stager behaves exactly like this formatter
    si.mySchema = myImpl->mySchema;
    si.myColTypes = myImpl->myColTypes;
    si.myMaxDepth = myImpl->myMaxDepth;
    si.myCheckColumns = myImpl->myCheckColumns;
    si.myExpectedAttrs = myImpl->myExpectedAttrs;
    si.myCurrentTag = myImpl->myCurrentTag;
    si.myWroteHeader = true;
    return stager;
}


void
ParquetFormatter::primeRowStager(OutputFormatter& stager) const {
    Impl& si = *static_cast<ParquetFormatter&>(stager).myImpl;
    // mirror the enclosing element state: the staged parent attributes prefix
    // every row and the tag stack drives the row size reset in closeTag
    si.myXMLStack = myImpl->myXMLStack;
    si.myRowSize = 0;
    for (int i = 0; i < myImpl->myRowSize; ++i) {
        si.nextSlot() = myImpl->myRow[i];
    }
    si.mySeenAttrs.reset();
    si.myNeedsWrite = false;
    if (si.myCurChunk == nullptr) {
        // the previous chunk was handed to the writer thread (see enqueueStagedRows);
        // recycle a consumed one to keep its buffer capacity
        si.myCurChunk = myImpl->takeFreeChunk();
    } else {
        si.myCurChunk->clear();
    }
    si.myConsumeRow = 0;
    si.myConsumeVal = 0;
}


void
ParquetFormatter::appendStagedRow(ParquetFormatter& stager) {
    Impl& si = *stager.myImpl;
    if (si.myCurChunk == nullptr || si.myConsumeRow >= si.myCurChunk->rows()) {
        throw ProcessError(TL("Trying to consume more rows than were staged."));
    }
    if (myImpl->myCurChunk == nullptr) {
        myImpl->myCurChunk = myImpl->takeFreeChunk();
    }
    Impl::RowChunk& dst = *myImpl->myCurChunk;
    const Impl::RowChunk& src = *si.myCurChunk;
    const int rowLen = src.rowLens[si.myConsumeRow];
    for (int i = 0; i < rowLen; ++i) {
        Impl::RowChunk::Val cv = src.vals[si.myConsumeVal + i];
        if (cv.kind == Impl::StagedValue::Kind::STR) {
            const unsigned strOff = (unsigned)dst.arena.size();
            dst.arena.append(src.arena, cv.strOff, cv.strLen);
            cv.strOff = strOff;
        }
        dst.vals.push_back(cv);
    }
    si.myConsumeVal += rowLen;
    si.myConsumeRow++;
    dst.rowLens.push_back(rowLen);
    // the merged row replaces a serial write of the same row (see closeTag)
    myImpl->mySeenAttrs.reset();
    myImpl->myNeedsWrite = false;
    if (dst.rows() >= Impl::CHUNK_ROWS) {
        myImpl->enqueueChunk();
    }
}


void
ParquetFormatter::enqueueStagedRows(ParquetFormatter& stager) {
    Impl& si = *stager.myImpl;
    if (si.myCurChunk == nullptr || si.myCurChunk->rows() == 0) {
        return;
    }
    // rows this formatter staged itself (e.g. earlier empty timesteps) come first
    myImpl->enqueueChunk();
    myImpl->pushChunk(std::move(si.myCurChunk));
    // the queued rows replace a serial write of the same rows (see closeTag)
    myImpl->mySeenAttrs.reset();
    myImpl->myNeedsWrite = false;
}


/****************************************************************************/
