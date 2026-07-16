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
/// @file    OutputDevice_RowStager.h
/// @author  Pranav Sateesh
/// @date    2026-07-16
///
// An output device which stages independent rows on a worker thread
/****************************************************************************/
#pragma once
#include <config.h>

#include <ostream>
#include <streambuf>
#include <string>
#include <vector>
#include "OutputDevice.h"


// ===========================================================================
// class definitions
// ===========================================================================
/**
 * @class OutputDevice_RowStager
 * @brief An output device which stages independent rows on a worker thread
 *
 * The device wraps a staging twin of a real device's formatter (see
 * OutputFormatter::createRowStager). A worker thread serializes rows into it
 * exactly as it would into the real device; the owning thread later appends
 * the staged rows in their original order via OutputDevice::appendStagedRow,
 * which keeps the output bitwise identical to a serial write.
 *
 * Stream based formatters (XML) write their bytes into an in-memory buffer
 * with recorded row boundaries; the Parquet formatter stages typed values in
 * its own chunk and only uses the stream for its precision state.
 */
class OutputDevice_RowStager : public OutputDevice {
public:
    /** @brief Constructor
     * @param[in] stagerFormatter The staging formatter (taken over by this device)
     */
    OutputDevice_RowStager(OutputFormatter* const stagerFormatter);

    /// @brief Destructor
    ~OutputDevice_RowStager();

    /// @brief records the end of the row currently being staged (worker thread)
    void endStagedRow() {
        myRowEnds.push_back(myBuffer.size());
    }

    /// @brief copies the bytes of the next unconsumed row into the given stream (owning thread)
    void writeNextRow(std::ostream& into) {
        const size_t end = myRowEnds[myNextRow++];
        into.write(myBuffer.data() + myConsumed, (std::streamsize)(end - myConsumed));
        myConsumed = end;
    }

    /// @brief drops all staged rows (called when the stager is primed for the next element)
    void reset() {
        myBuffer.clearBuffer();
        myRowEnds.clear();
        myNextRow = 0;
        myConsumed = 0;
    }

protected:
    /// @brief Returns the associated ostream
    std::ostream& getOStream() override {
        return myStream;
    }

private:
    /// @brief a string backed stream buffer allowing access to its data without copying
    class StringBuffer : public std::streambuf {
    public:
        const char* data() const {
            return myData.data();
        }
        size_t size() const {
            return myData.size();
        }
        void clearBuffer() {
            myData.clear();
        }
    protected:
        std::streamsize xsputn(const char* s, std::streamsize n) override {
            myData.append(s, (size_t)n);
            return n;
        }
        int overflow(int c) override {
            if (c != EOF) {
                myData.push_back((char)c);
            }
            return c;
        }
    private:
        std::string myData;
    };

    /// @brief the buffer receiving the staged bytes (stream based formatters)
    StringBuffer myBuffer;

    /// @brief the stream handed to the formatter (also carries the precision state)
    std::ostream myStream;

    /// @brief the end offset of each staged row in the buffer
    std::vector<size_t> myRowEnds;

    /// @brief the next row to be consumed and the number of bytes consumed so far
    size_t myNextRow = 0;
    size_t myConsumed = 0;
};
