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
/// @file    OutputDevice_RowStager.cpp
/// @author  Pranav Sateesh
/// @date    2026-07-16
///
// An output device which stages independent rows on a worker thread
/****************************************************************************/
#include <config.h>

#include <iomanip>
#include "OutputDevice_RowStager.h"


// ===========================================================================
// method definitions
// ===========================================================================
OutputDevice_RowStager::OutputDevice_RowStager(OutputFormatter* const stagerFormatter)
    : OutputDevice(0, "rowstager"), myStream(&myBuffer) {
    setFormatter(stagerFormatter);
    setPrecision();
    myStream << std::setiosflags(std::ios::fixed);
}


OutputDevice_RowStager::~OutputDevice_RowStager() {
}
