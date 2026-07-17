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
/// @file    MSFCDExport.cpp
/// @author  Daniel Krajzewicz
/// @author  Jakob Erdmann
/// @author  Mario Krumnow
/// @author  Michael Behrisch
/// @author  Pranav Sateesh
/// @date    2012-04-26
///
// Realises dumping Floating Car Data (FCD) Data
/****************************************************************************/
#include <config.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <utils/common/MsgHandler.h>
#include <utils/iodevices/OutputDevice.h>
#include <utils/iodevices/OutputDevice_RowStager.h>
#include <utils/options/OptionsCont.h>
#include <utils/geom/GeoConvHelper.h>
#include <utils/geom/GeomHelper.h>
#include <utils/shapes/SUMOPolygon.h>
#include <libsumo/Helper.h>
#include <microsim/devices/MSDevice_FCD.h>
#include <microsim/devices/MSTransportableDevice_FCD.h>
#include <microsim/MSEdgeControl.h>
#include <microsim/MSEdge.h>
#include <microsim/MSLane.h>
#include <microsim/MSGlobals.h>
#include <microsim/MSNet.h>
#include <microsim/MSVehicle.h>
#include <microsim/lcmodels/MSAbstractLaneChangeModel.h>
#include <microsim/transportables/MSPerson.h>
#include <microsim/transportables/MSTransportableControl.h>
#include <microsim/MSVehicleControl.h>
#include <mesosim/MEVehicle.h>
#include "MSEmissionExport.h"
#include "MSFCDExport.h"


// ===========================================================================
// FCDWorkerPool definition
// ===========================================================================
namespace {
/// @brief the emission attributes which are computed eagerly by MSEmissionExport::writeEmissions
SumoXMLAttrMask
eagerEmissionAttributes() {
    SumoXMLAttrMask mask;
    mask.set(SUMO_ATTR_CO2);
    mask.set(SUMO_ATTR_CO);
    mask.set(SUMO_ATTR_HC);
    mask.set(SUMO_ATTR_NOX);
    mask.set(SUMO_ATTR_PMX);
    mask.set(SUMO_ATTR_FUEL);
    mask.set(SUMO_ATTR_ELECTRICITY);
    return mask;
}


/** @brief A persistent pool of worker threads which takes the expensive part
 * of the FCD output off the writing thread while the rows are still written
 * in their original order.
 *
 * Two kinds of jobs exist: VALUES computes the per-vehicle values (position,
 * angle, slope, geo projection) which the writing thread then serializes, and
 * SERIALIZE runs the complete row serialization against per-thread staging
 * devices (see OutputDevice::createRowStager) so the writing thread only
 * appends the finished rows. Both keep the output bitwise identical to a
 * single threaded run because exactly the same code runs on each vehicle;
 * only the loop is partitioned.
 */
class FCDWorkerPool {
public:
    ~FCDWorkerPool() {
        shutdown();
    }

    /// @brief bring up the workers and projection copies; returns whether the copies are usable
    bool prepare(const int numThreads, const bool useGeo) {
        // all slices run on workers; the calling thread only publishes the job,
        // appends the results and continues with the simulation
        startWorkers(numThreads, useGeo);
        return myClonesValid;
    }

    /// @brief fill in the values of all rows (VALUES job); prepare() must have been called
    void computeAll(std::vector<MSFCDExport::VehicleState>& rows, const SumoXMLAttrMask& mask,
                    const bool useGeo, const bool useUTM) {
        runJob(rows, Job::VALUES, mask, useGeo && myClonesValid, useUTM, -1., nullptr);
        if (useGeo && !myClonesValid) {
            // the projection could not be duplicated for the workers; convert here instead
            for (MSFCDExport::VehicleState& row : rows) {
                GeoConvHelper::getFinal().cartesian2geo(row.pos);
            }
        }
    }

    /// @brief serialize all rows into per-thread staging devices (SERIALIZE job); prepare() must have been called
    void serializeAll(OutputDevice& of, std::vector<MSFCDExport::VehicleState>& rows, const SumoXMLAttrMask& mask,
                      const bool useGeo, const bool useUTM, const double maxLeaderDistance,
                      const std::vector<std::string>& params) {
        if (myStagers.empty()) {
            for (int i = 0; i < (int)myThreads.size(); i++) {
                myStagers.push_back(of.createRowStager());
            }
        }
        for (OutputDevice* const stager : myStagers) {
            of.primeRowStager(*stager);
        }
        runJob(rows, Job::SERIALIZE, mask, useGeo, useUTM, maxLeaderDistance, &params);
    }

    /// @brief append all staged rows slice by slice (only valid when no other output interleaves)
    void appendAllStaged(OutputDevice& of) {
        for (OutputDevice* const stager : myStagers) {
            of.appendAllStagedRows(*stager);
        }
    }

    /// @brief the staging device which serialized the given row
    OutputDevice& stagerForRow(const int rowIdx, const int numRows) const {
        const long long int numParts = (long long int)myThreads.size();
        int part = (int)((long long int)rowIdx * numParts / numRows);
        while ((long long int)numRows * (part + 1) / numParts <= rowIdx) {
            part++;
        }
        return *myStagers[part];
    }

    /// @brief join all workers, release the projection copies and staging devices
    void shutdown() {
        if (!myThreads.empty()) {
            {
                std::lock_guard<std::mutex> lock(myMutex);
                myShutdown = true;
            }
            myCondition.notify_all();
            for (std::thread& t : myThreads) {
                t.join();
            }
            myThreads.clear();
            myShutdown = false;
        }
        for (GeoConvHelper* const clone : myGeoClones) {
            delete clone;
        }
        myGeoClones.clear();
        myHaveClones = false;
        for (OutputDevice* const stager : myStagers) {
            delete stager;
        }
        myStagers.clear();
    }

private:
    /// @brief the kind of work performed by a job generation
    enum class Job { VALUES, SERIALIZE };

    /// @brief publish a job to the workers and wait for its completion
    void runJob(std::vector<MSFCDExport::VehicleState>& rows, const Job job, const SumoXMLAttrMask& mask,
                const bool doGeo, const bool doUTM, const double maxLeaderDistance,
                const std::vector<std::string>* const params) {
        {
            std::lock_guard<std::mutex> lock(myMutex);
            myRows = &rows;
            myJob = job;
            myDoGeo = doGeo;
            myDoUTM = doUTM;
            myUTMOffset = GeoConvHelper::getFinal().getOffset();
            myMask = mask;
            myMaxLeaderDistance = maxLeaderDistance;
            myParams = params;
            myUnfinished = (int)myThreads.size();
            myGeneration++;
        }
        myCondition.notify_all();
        std::string error;
        {
            std::unique_lock<std::mutex> lock(myMutex);
            myDone.wait(lock, [this] {
                return myUnfinished == 0;
            });
            error = myError;
            myError = "";
        }
        myRows = nullptr;
        if (error != "") {
            throw ProcessError(error);
        }
    }

    /// @brief run one slice of the current job
    void runSlice(const int part) {
        if (myJob == Job::VALUES) {
            computeSlice(part);
        } else {
            serializeSlice(part);
        }
    }
    /// @brief bring up the workers (and the projection copies) if not running yet
    void startWorkers(const int numWorkers, const bool useGeo) {
        if ((int)myThreads.size() == numWorkers && (!useGeo || myHaveClones)) {
            return;
        }
        shutdown();
        myClonesValid = true;
        if (useGeo) {
            myHaveClones = true;
            for (int i = 0; i < numWorkers; i++) {
                GeoConvHelper* const clone = GeoConvHelper::getFinal().makeThreadSafeCopy();
                if (clone == nullptr) {
                    myClonesValid = false;
                }
                myGeoClones.push_back(clone);
            }
            if (!myClonesValid) {
                WRITE_WARNING(TL("Could not duplicate the projection for parallel FCD output; the geo conversion runs in a single thread."));
            }
        } else {
            myGeoClones.assign(numWorkers, nullptr);
        }
        for (int i = 0; i < numWorkers; i++) {
            myThreads.emplace_back(&FCDWorkerPool::workerMain, this, i);
        }
    }

    /// @brief compute the values of one contiguous slice of the rows
    void computeSlice(const int part) {
        std::vector<MSFCDExport::VehicleState>& rows = *myRows;
        const long long int numRows = (long long int)rows.size();
        const long long int numParts = (long long int)myThreads.size();
        const int begin = (int)(numRows * part / numParts);
        const int end = (int)(numRows * (part + 1) / numParts);
        const GeoConvHelper* const geo = myGeoClones[part];
        for (int i = begin; i < end; i++) {
            MSFCDExport::VehicleState& row = rows[i];
            row.pos = row.veh->getPosition();
            if (myDoGeo) {
                geo->cartesian2geo(row.pos);
            } else if (myDoUTM) {
                row.pos.sub(myUTMOffset);
            }
            if (myMask.test(SUMO_ATTR_ANGLE)) {
                row.angle = GeomHelper::naviDegree(row.veh->getAngle());
            }
            if (myMask.test(SUMO_ATTR_SLOPE)) {
                row.slope = row.veh->getSlope();
            }
            if (myMask.test(SUMO_ATTR_POSITION)) {
                row.posOnLane = row.veh->getPositionOnLane();
            }
        }
    }

    /// @brief serialize one contiguous slice of the rows into this slice's staging device
    void serializeSlice(const int part) {
        std::vector<MSFCDExport::VehicleState>& rows = *myRows;
        const long long int numRows = (long long int)rows.size();
        const long long int numParts = (long long int)myThreads.size();
        const int begin = (int)(numRows * part / numParts);
        const int end = (int)(numRows * (part + 1) / numParts);
        const GeoConvHelper* const geo = myGeoClones[part];
        OutputDevice_RowStager* const stager = static_cast<OutputDevice_RowStager*>(myStagers[part]);
        for (int i = begin; i < end; i++) {
            MSFCDExport::writeVehicle(*stager, rows[i].veh, nullptr, myMask, myDoGeo, myDoUTM,
                                      myMaxLeaderDistance, *myParams, geo);
            stager->endStagedRow();
        }
    }

    /// @brief the worker loop: wait for the next job generation, run the assigned slice, count down
    void workerMain(const int part) {
        long long int lastGeneration = 0;
        while (true) {
            {
                std::unique_lock<std::mutex> lock(myMutex);
                myCondition.wait(lock, [this, lastGeneration] {
                    return myShutdown || myGeneration != lastGeneration;
                });
                if (myShutdown) {
                    return;
                }
                lastGeneration = myGeneration;
            }
            try {
                runSlice(part);
            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> lock(myMutex);
                if (myError == "") {
                    myError = e.what();
                }
            }
            {
                std::lock_guard<std::mutex> lock(myMutex);
                if (--myUnfinished == 0) {
                    myDone.notify_all();
                }
            }
        }
    }

    /// @brief the worker threads
    std::vector<std::thread> myThreads;

    /// @brief one staging device per worker, created on first use
    std::vector<OutputDevice*> myStagers;

    /// @brief one projection copy per worker (entries may be nullptr)
    std::vector<GeoConvHelper*> myGeoClones;

    /// @brief whether projection copies were requested for the current workers
    bool myHaveClones = false;

    /// @brief whether every worker got a usable projection copy
    bool myClonesValid = true;

    /// @brief protects the job state below
    std::mutex myMutex;

    /// @brief signals a new job generation to the workers
    std::condition_variable myCondition;

    /// @brief signals the completion of all slices to the caller
    std::condition_variable myDone;

    /// @brief the current job generation
    long long int myGeneration = 0;

    /// @brief the number of slices still being computed
    int myUnfinished = 0;

    /// @brief whether the workers shall terminate
    bool myShutdown = false;

    /// @brief the first error thrown by a worker (if any)
    std::string myError;

    /// @{ the description of the current job
    std::vector<MSFCDExport::VehicleState>* myRows = nullptr;
    Job myJob = Job::VALUES;
    bool myDoGeo = false;
    bool myDoUTM = false;
    Position myUTMOffset;
    SumoXMLAttrMask myMask;
    double myMaxLeaderDistance = -1.;
    const std::vector<std::string>* myParams = nullptr;
    /// @}
};

FCDWorkerPool gFCDPool;
}


// ===========================================================================
// method definitions
// ===========================================================================
void
MSFCDExport::write(OutputDevice& of, const SUMOTime timestep, const SumoXMLTag tag) {
    MSDevice_FCD::initOnce();
    const SUMOTime period = MSDevice_FCD::getPeriod();
    const SUMOTime begin = MSDevice_FCD::getBegin();
    if ((period > 0 && (timestep - begin) % period != 0) || timestep < begin) {
        return;
    }
    const SumoXMLAttrMask& mask = MSDevice_FCD::getWrittenAttributes();
    const bool useGeo = MSDevice_FCD::useGeo();
    const bool useUTM = MSDevice_FCD::useUTM();
    const double maxLeaderDistance = MSDevice_FCD::getMaxLeaderDistance();
    const std::vector<std::string>& params = MSDevice_FCD::getParamsToWrite();
    MSNet* net = MSNet::getInstance();
    MSVehicleControl& vc = net->getVehicleControl();
    const double radius = MSDevice_FCD::getRadius();
    const bool filter = MSDevice_FCD::getEdgeFilter().size() > 0;
    const bool shapeFilter = MSDevice_FCD::hasShapeFilter();
    std::set<const Named*> inRadius;
    if (radius > 0) {
        // collect all vehicles in radius around equipped vehicles
        for (MSVehicleControl::constVehIt it = vc.loadedVehBegin(); it != vc.loadedVehEnd(); ++it) {
            const SUMOVehicle* veh = it->second;
            if (isVisible(veh) && hasOwnOutput(veh, filter, shapeFilter)) {
                PositionVector shape;
                shape.push_back(veh->getPosition());
                libsumo::Helper::collectObjectsInRange(libsumo::CMD_GET_VEHICLE_VARIABLE, shape, radius, inRadius);
                libsumo::Helper::collectObjectsInRange(libsumo::CMD_GET_PERSON_VARIABLE, shape, radius, inRadius);
            }
        }
    }

    bool wroteTimestep = false;
    auto openTimestep = [&]() {
        if (!wroteTimestep) {
            of.openTag("timestep").writeTime(SUMO_ATTR_TIME, timestep);
            wroteTimestep = true;
        }
    };

    const bool writeVehicles = tag == SUMO_TAG_NOTHING || tag == SUMO_TAG_VEHICLE;
    const bool parallel = writeVehicles && MSDevice_FCD::getThreads() > 1;
    std::vector<VehicleState> precomputed;
    std::vector<VehicleState>::size_type nextRow = 0;
    bool staged = false;
    bool bulkAppended = false;
    if (parallel) {
        for (MSVehicleControl::constVehIt it = vc.loadedVehBegin(); it != vc.loadedVehEnd(); ++it) {
            const SUMOVehicle* const veh = it->second;
            if (isVisible(veh) && hasOwnOutput(veh, filter, shapeFilter, radius > 0 && inRadius.count(veh) > 0)) {
                if (MSGlobals::gUseMesoSim && MSGlobals::gMesoInterpolatePos) {
                    // fill the per edge position cache while still single threaded
                    veh->getEdge()->getMesoPositions();
                }
                precomputed.push_back(VehicleState());
                precomputed.back().veh = veh;
            }
        }
        if (!precomputed.empty()) {
            const bool geoOK = gFCDPool.prepare(MSDevice_FCD::getThreads(), useGeo);
            // rows may only be serialized on the workers when everything they
            // evaluate is safe off the main thread; otherwise fall back to
            // precomputing the values and serializing here
            static const SumoXMLAttrMask eagerEmissions = eagerEmissionAttributes();
            staged = of.supportsParallelRows() && params.empty() && maxLeaderDistance < 0
                     && !(mask & eagerEmissions).any() && (!useGeo || geoOK);
            if (staged) {
                // the timestep element must be open so the stagers can copy its state
                openTimestep();
                gFCDPool.serializeAll(of, precomputed, mask, useGeo, useUTM, maxLeaderDistance, params);
                // without active transportables nothing can interleave with the vehicle
                // rows, so whole slices are appended at once (for Parquet this is a
                // queue push to the writer thread instead of copying every row) and
                // the second loop over the vehicles is skipped entirely
                const bool mayInterleave = tag == SUMO_TAG_NOTHING
                                           && ((net->hasPersons() && net->getPersonControl().hasTransportables())
                                               || (net->hasContainers() && net->getContainerControl().hasTransportables()));
                if (!mayInterleave) {
                    gFCDPool.appendAllStaged(of);
                    bulkAppended = true;
                }
            } else {
                gFCDPool.computeAll(precomputed, mask, useGeo, useUTM);
            }
        }
    }

    if (!MSDevice_FCD::skipEmpty()) {
        openTimestep();
    }
    // when the rows were appended in bulk there is nothing left to write per vehicle
    for (MSVehicleControl::constVehIt it = vc.loadedVehBegin(); !bulkAppended && it != vc.loadedVehEnd(); ++it) {
        const SUMOVehicle* const veh = it->second;
        if (isVisible(veh)) {
            const VehicleState* pre = nullptr;
            bool hasOutput;
            if (parallel) {
                // the precomputed rows are a subsequence of this loop so a pointer comparison
                // avoids evaluating the filters a second time
                hasOutput = nextRow < precomputed.size() && precomputed[nextRow].veh == veh;
                if (hasOutput) {
                    pre = &precomputed[nextRow];
                    nextRow++;
                }
            } else {
                hasOutput = writeVehicles && hasOwnOutput(veh, filter, shapeFilter, radius > 0 && inRadius.count(veh) > 0);
            }
            if (hasOutput) {
                openTimestep();
                if (staged) {
                    of.appendStagedRow(gFCDPool.stagerForRow((int)nextRow - 1, (int)precomputed.size()));
                } else {
                    writeVehicle(of, veh, pre, mask, useGeo, useUTM, maxLeaderDistance, params, &GeoConvHelper::getFinal());
                }
            }
            // write persons and containers in the vehicle
            if (tag == SUMO_TAG_NOTHING || tag == SUMO_TAG_PERSON) {
                const MSEdge* edge = MSGlobals::gUseMesoSim ? veh->getEdge() : &veh->getLane()->getEdge();
                for (const MSTransportable* const person : veh->getPersons()) {
                    if (hasOwnOutput(person, filter, shapeFilter, inRadius.count(person) > 0)) {
                        openTimestep();
                        writeTransportable(of, edge, person, veh, SUMO_TAG_PERSON, useGeo, mask);
                    }
                }
                for (const MSTransportable* const container : veh->getContainers()) {
                    if (hasOwnOutput(container, filter, shapeFilter, inRadius.count(container) > 0)) {
                        openTimestep();
                        writeTransportable(of, edge, container, veh, SUMO_TAG_CONTAINER, useGeo, mask);
                    }
                }
            }
        }
    }
    if (tag == SUMO_TAG_NOTHING || tag == SUMO_TAG_PERSON) {
        if (net->hasPersons() && net->getPersonControl().hasTransportables()) {
            // write persons who are not in a vehicle
            for (const MSEdge* const e : net->getEdgeControl().getEdges()) {
                if (filter && MSDevice_FCD::getEdgeFilter().count(e) == 0) {
                    continue;
                }
                for (const MSTransportable* const person : e->getSortedPersons(timestep)) {
                    if (hasOwnOutput(person, filter, shapeFilter, inRadius.count(person) > 0)) {
                        openTimestep();
                        writeTransportable(of, e, person, nullptr, SUMO_TAG_PERSON, useGeo, mask);
                    }
                }
            }
        }
        if (net->hasContainers() && net->getContainerControl().hasTransportables()) {
            // write containers which are not in a vehicle
            for (const MSEdge* const e : net->getEdgeControl().getEdges()) {
                if (filter && MSDevice_FCD::getEdgeFilter().count(e) == 0) {
                    continue;
                }
                for (MSTransportable* container : e->getSortedContainers(timestep)) {
                    if (hasOwnOutput(container, filter, shapeFilter, inRadius.count(container) > 0)) {
                        openTimestep();
                        writeTransportable(of, e, container, nullptr, SUMO_TAG_CONTAINER, useGeo, mask);
                    }
                }
            }
        }
    }
    if (wroteTimestep) {
        of.closeTag();
    }
}


void
MSFCDExport::cleanup() {
    gFCDPool.shutdown();
}


void
MSFCDExport::writeVehicle(OutputDevice& of, const SUMOVehicle* const veh, const VehicleState* const pre,
                          const SumoXMLAttrMask& mask, const bool useGeo, const bool useUTM,
                          const double maxLeaderDistance, const std::vector<std::string>& params,
                          const GeoConvHelper* const geo) {
    const MSVehicle* const microVeh = MSGlobals::gUseMesoSim ? nullptr : static_cast<const MSVehicle*>(veh);
    Position pos;
    if (pre != nullptr) {
        pos = pre->pos;
    } else {
        pos = veh->getPosition();
        if (useGeo) {
            geo->cartesian2geo(pos);
        } else if (useUTM) {
            pos.sub(geo->getOffset());
        }
    }
    if (useGeo) {
        of.setPrecision(gPrecisionGeo);
    }
    of.openTag(SUMO_TAG_VEHICLE);
    of.writeAttr(SUMO_ATTR_ID, veh->getID());
    of.writeOptionalAttr(SUMO_ATTR_X, pos.x(), mask);
    of.writeOptionalAttr(SUMO_ATTR_Y, pos.y(), mask);
    of.setPrecision(gPrecision);
    of.writeOptionalAttr(SUMO_ATTR_Z, pos.z(), mask);
    of.writeFuncAttr(SUMO_ATTR_ANGLE, [ = ]() {
        return pre != nullptr ? pre->angle : GeomHelper::naviDegree(veh->getAngle());
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_TYPE, [ = ]() {
        return veh->getVehicleType().getID();
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_SPEED, [ = ]() {
        return veh->getSpeed();
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_SPEEDREL, [ = ]() {
        const double speedLimit = veh->getEdge()->getSpeedLimit();
        return speedLimit > 0 ? veh->getSpeed() / speedLimit : 0.;
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_POSITION, [ = ]() {
        return pre != nullptr ? pre->posOnLane : veh->getPositionOnLane();
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_LANE, [ = ]() {
        return MSGlobals::gUseMesoSim ? "" : microVeh->getLane()->getID();
    }, mask, MSGlobals::gUseMesoSim);
    of.writeFuncAttr(SUMO_ATTR_EDGE, [ = ]() {
        return veh->getCurrentEdge()->getID();
    }, mask, !MSGlobals::gUseMesoSim);
    of.writeFuncAttr(SUMO_ATTR_SLOPE, [ = ]() {
        return pre != nullptr ? pre->slope : veh->getSlope();
    }, mask);
    if (!MSGlobals::gUseMesoSim) {
        of.writeFuncAttr(SUMO_ATTR_SIGNALS, [ = ]() {
            return microVeh->getSignals();
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_ACCELERATION, [ = ]() {
            return microVeh->getAcceleration();
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_ACCELERATION_LAT, [ = ]() {
            return microVeh->getLaneChangeModel().getAccelerationLat();
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_SPEED_VEC, [ = ]() {
            return GeomHelper::vectorize(microVeh->getSpeed(), microVeh->getAngle());
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_ACCEL_VEC, [ = ]() {
            return GeomHelper::vectorize(microVeh->getAcceleration(), microVeh->getAngle());
        }, mask);
    }
    of.writeFuncAttr(SUMO_ATTR_DISTANCE, [ = ]() {
        double lanePos = veh->getPositionOnLane();
        if (!MSGlobals::gUseMesoSim && microVeh->getLane()->isInternal()) {
            lanePos = microVeh->getRoute().getDistanceBetween(0., lanePos, microVeh->getEdge()->getLanes()[0], microVeh->getLane(),
                      microVeh->getRoutePosition());
        }
        return veh->getEdge()->getDistanceAt(lanePos);
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_ODOMETER, [ = ]() {
        return veh->getOdometer();
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_POSITION_LAT, [ = ]() {
        return veh->getLateralPositionOnLane();
    }, mask);
    if (!MSGlobals::gUseMesoSim) {
        of.writeFuncAttr(SUMO_ATTR_SPEED_LAT, [ = ]() {
            return microVeh->getLaneChangeModel().getSpeedLat();
        }, mask);
    }
    if (maxLeaderDistance >= 0 && !MSGlobals::gUseMesoSim) {
        const std::pair<const MSVehicle* const, double> leader = microVeh->getLeader(maxLeaderDistance);
        if (leader.first != nullptr) {
            of.writeFuncAttr(SUMO_ATTR_LEADER_ID, [ = ]() {
                return leader.first->getID();
            }, mask);
            of.writeFuncAttr(SUMO_ATTR_LEADER_SPEED, [ = ]() {
                return leader.first->getSpeed();
            }, mask);
            of.writeFuncAttr(SUMO_ATTR_LEADER_GAP, [ = ]() {
                return leader.second + microVeh->getVehicleType().getMinGap();
            }, mask);
        } else {
            of.writeFuncAttr(SUMO_ATTR_LEADER_ID, [ = ]() {
                return "";
            }, mask);
            of.writeFuncAttr(SUMO_ATTR_LEADER_SPEED, [ = ]() {
                return -1;
            }, mask);
            of.writeFuncAttr(SUMO_ATTR_LEADER_GAP, [ = ]() {
                return -1;
            }, mask);
        }
    }
    for (const std::string& key : params) {
        std::string error;
        const std::string value = static_cast<const MSBaseVehicle*>(veh)->getPrefixedParameter(key, error);
        if (value != "") {
            of.writeAttr(StringUtils::escapeXML(key), StringUtils::escapeXML(value));
        }
    }
    of.writeFuncAttr(SUMO_ATTR_ARRIVALDELAY, [ = ]() {
        const double arrivalDelay = static_cast<const MSBaseVehicle*>(veh)->getStopArrivalDelay();
        if (arrivalDelay == INVALID_DOUBLE) {
            // no upcoming stop also means that there is no delay
            return 0.;
        }
        return arrivalDelay;
    }, mask);
    of.writeFuncAttr(SUMO_ATTR_DELAY, [ = ]() {
        const double delay = static_cast<const MSBaseVehicle*>(veh)->getStopDelay();
        if (delay < 0) {
            // no upcoming stop also means that there is no delay
            return 0.;
        }
        return delay;
    }, mask);
    if (MSGlobals::gUseMesoSim) {
        const MEVehicle* mesoVeh = static_cast<const MEVehicle*>(veh);
        of.writeFuncAttr(SUMO_ATTR_SEGMENT, [ = ]() {
            return mesoVeh->getSegmentIndex();
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_QUEUE, [ = ]() {
            return mesoVeh->getQueIndex();
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_ENTRYTIME, [ = ]() {
            return mesoVeh->getLastEntryTimeSeconds();
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_EVENTTIME, [ = ]() {
            return mesoVeh->getEventTimeSeconds();
        }, mask);
        of.writeFuncAttr(SUMO_ATTR_BLOCKTIME, [ = ]() {
            return mesoVeh->getBlockTime() == SUMOTime_MAX ? -1.0 : mesoVeh->getBlockTimeSeconds();
        }, mask);
    }
    of.writeFuncAttr(SUMO_ATTR_TAG, [ = ]() {
        return toString(SUMO_TAG_VEHICLE);
    }, mask);
    of.writeOptionalAttr(SUMO_ATTR_PERSON_NUMBER, veh->getPersonNumber(), mask);
    of.writeOptionalAttr(SUMO_ATTR_CONTAINER_NUMBER, veh->getContainerNumber(), mask);
    MSEmissionExport::writeEmissions(of, static_cast<const MSBaseVehicle*>(veh), false, mask);
    of.closeTag();
}


bool
MSFCDExport::isVisible(const SUMOVehicle* veh) {
    return veh->isOnRoad() || veh->isParking() || veh->isRemoteControlled();
}


bool
MSFCDExport::hasOwnOutput(const SUMOVehicle* veh, bool filter, bool shapeFilter, bool isInRadius) {
    return ((!filter || MSDevice_FCD::getEdgeFilter().count(veh->getEdge()) > 0)
            && (!shapeFilter || MSDevice_FCD::shapeFilter(veh))
            && ((veh->getDevice(typeid(MSDevice_FCD)) != nullptr) || isInRadius));
}


bool
MSFCDExport::hasOwnOutput(const MSTransportable* p, bool filter, bool shapeFilter, bool isInRadius) {
    return ((!filter || MSDevice_FCD::getEdgeFilter().count(p->getEdge()) > 0)
            && (!shapeFilter || MSDevice_FCD::shapeFilter(p))
            && ((p->getDevice(typeid(MSTransportableDevice_FCD)) != nullptr) || isInRadius));
}


void
MSFCDExport::writeTransportable(OutputDevice& of, const MSEdge* const e, const MSTransportable* const p, const SUMOVehicle* const v,
                                const SumoXMLTag tag, const bool useGeo, const SumoXMLAttrMask mask) {
    Position pos = p->getPosition();
    if (useGeo) {
        of.setPrecision(gPrecisionGeo);
        GeoConvHelper::getFinal().cartesian2geo(pos);
    }
    of.openTag(tag);
    of.writeAttr(SUMO_ATTR_ID, p->getID());
    of.writeOptionalAttr(SUMO_ATTR_X, pos.x(), mask);
    of.writeOptionalAttr(SUMO_ATTR_Y, pos.y(), mask);
    of.setPrecision(gPrecision);
    of.writeOptionalAttr(SUMO_ATTR_Z, pos.z(), mask);
    of.writeOptionalAttr(SUMO_ATTR_ANGLE, GeomHelper::naviDegree(p->getAngle()), mask);
    of.writeOptionalAttr(SUMO_ATTR_TYPE, p->getVehicleType().getID(), mask);
    of.writeOptionalAttr(SUMO_ATTR_SPEED, p->getSpeed(), mask);
    of.writeOptionalAttr(SUMO_ATTR_SPEEDREL, e->getSpeedLimit() > 0 ? p->getSpeed() / e->getSpeedLimit() : 0., mask);
    of.writeOptionalAttr(SUMO_ATTR_POSITION, p->getEdgePos(), mask);
    of.writeOptionalAttr(SUMO_ATTR_LANE, "", mask, true);
    of.writeOptionalAttr(SUMO_ATTR_EDGE, e->getID(), mask);
    of.writeOptionalAttr(SUMO_ATTR_SLOPE, e->getLanes()[0]->getShape().slopeDegreeAtOffset(p->getEdgePos()), mask);
    of.writeOptionalAttr(SUMO_ATTR_VEHICLE, v == nullptr ? "" : v->getID(), mask);
    of.writeOptionalAttr(SUMO_ATTR_STAGE, p->getCurrentStageDescription(), mask);
    of.writeOptionalAttr(SUMO_ATTR_TAG, toString(tag), mask);
    of.closeTag();
}


/****************************************************************************/
