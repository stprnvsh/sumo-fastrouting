/****************************************************************************/
// Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.dev/sumo
// Copyright (C) 2001-2026 German Aerospace Center (DLR) and others.
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
/// @file    MELoop.cpp
/// @author  Daniel Krajzewicz
/// @date    Tue, May 2005
///
// The main mesocopic simulation loop
/****************************************************************************/
#include <config.h>

#include <queue>
#include <vector>
#include <map>
#include <cmath>
#include <algorithm>

#ifdef HAVE_FOX
#include <utils/foxtools/MFXWorkerThread.h>
#endif
#include <microsim/MSNet.h>
#include <microsim/MSEdge.h>
#include <microsim/MSEdgeControl.h>
#include <microsim/MSGlobals.h>
#include <microsim/MSLane.h>
#include <microsim/MSVehicle.h>
#include <microsim/MSVehicleControl.h>
#include <utils/options/OptionsCont.h>
#include <utils/common/ToString.h>
#include <utils/common/FileHelpers.h>
#include <utils/common/SUMOTime.h>
#include <utils/common/RandHelper.h>
#include "MELoop.h"
#include "MESegment.h"
#include "MELSegment.h"
#include "MEVehicle.h"

long long int MELoop::LeaderEvent::myEventCounter(0);
thread_local MELoop::DeferredOps* MELoop::myDeferred(nullptr);
const int MELoop::MIN_PARALLEL_MOVES(64);

// ===========================================================================
// method definitions
// ===========================================================================
MELoop::MELoop(const SUMOTime recheckInterval) : myFullRecheckInterval(recheckInterval), myLinkRecheckInterval(TIME2STEPS(1)) {
}

MELoop::~MELoop() {
    for (std::vector<MESegment*>::const_iterator j = myEdges2FirstSegments.begin(); j != myEdges2FirstSegments.end(); ++j) {
        for (MESegment* s = *j; s != nullptr;) {
            MESegment* n = s->getNextSegment();
            delete s;
            s = n;
        }
    }
}


void
MELoop::simulate(SUMOTime tMax) {
    if (MSGlobals::gMesoParallelLoop) {
        simulateParallel(tMax);
        return;
    }
    while (!myLeaderCars.empty()) {
        LeaderEvent e = myLeaderCars.top();
        assert(e.time > tMax - DELTA_T);
        if (e.time > tMax) {
            return;
        }
        myLeaderCars.pop();
        if (skipInvalidated(e)) {
            continue;
        }
        //std::cout << SIMTIME << " checkChar " << e.veh->getID() << " on=" << e.veh->getSegment()->getID() << " t=" << e.time << " nid=" << e.veh->getNumericalID() << "\n";
        checkCar(e.veh);
        assert(myLeaderCars.empty() || myLeaderCars.top().time >= e.time);
    }
}


bool
MELoop::skipInvalidated(const LeaderEvent& e) {
    while (!myInvalidatedLeaderCars.empty() && e.time > myInvalidatedLeaderCars.top().time) {
        myInvalidatedLeaderCars.pop();
    }
    return !myInvalidatedLeaderCars.empty() && wasInvalidated(e);
}


bool
MELoop::wasInvalidated(const LeaderEvent& e) {
    if (e.time == myInvalidatedLeaderCars.top().time) {
        // we need to check whether any of the events with the same time
        // is our current vehicle because the order in which
        // removeLeaderCar was called is unrelated to the order of addLeaderCar
        std::vector<LeaderEvent> tmp;
        bool curInvalid = false;
        while (!myInvalidatedLeaderCars.empty() && e.time == myInvalidatedLeaderCars.top().time) {
            if (e == myInvalidatedLeaderCars.top()) {
                myInvalidatedLeaderCars.pop();
                curInvalid = true;
                break;
            } else {
                tmp.push_back(myInvalidatedLeaderCars.top());
                myInvalidatedLeaderCars.pop();
            }
        }
        for (LeaderEvent& le : tmp) {
            myInvalidatedLeaderCars.push(le);
        }
        return curInvalid;
    }
    return false;
}


SUMOTime
MELoop::changeSegment(MEVehicle* veh, SUMOTime leaveTime, MESegment* const toSegment, MSMoveReminder::Notification reason, const bool ignoreLink) const {
    int qIdx = 0;
    MESegment* const onSegment = veh->getSegment();
    if (MESegment::isInvalid(toSegment)) {
        if (veh->isStoppedTriggered()) {
            return leaveTime + MAX2(SUMOTime(1), myLinkRecheckInterval);
        }
        if (onSegment != nullptr) {
            onSegment->send(veh, toSegment, qIdx, leaveTime, reason);
        } else {
            WRITE_WARNINGF(TL("Vehicle '%' teleports beyond arrival edge '%', time=%."),
                           veh->getID(), veh->getEdge()->getID(), time2string(leaveTime));
        }
        veh->setSegment(toSegment); // signal arrival
        MSNet::getInstance()->getVehicleControl().scheduleVehicleRemoval(veh);
        return leaveTime;
    } else if ((!MSGlobals::gCheckRoutes || veh->ignoreTransientPermissions())
               && !ignoreLink
               && !MESegment::isInvalid(onSegment)
               && &onSegment->getEdge() != &toSegment->getEdge()
               && veh->getEdge()->allowedLanes(*veh->succEdge(1), veh->getVClass()) == nullptr) {
        if (veh->isStopped()) {
            veh->processStop();
        }
        // all queues on the next segment are forbidden. We don't want to check
        // too often whether the forbiden edge has become permitted again
        if (MSGlobals::gTimeToGridlock > 0) {
            // if teleporting is enabled, make sure we look at the vehicle when the gridlock-time is up
            const SUMOTime recheck = MSGlobals::gTimeToTeleportDisconnected >= 0 ? MIN2(MSGlobals::gTimeToGridlock, MSGlobals::gTimeToTeleportDisconnected) : MSGlobals::gTimeToGridlock;
            return MAX2(MIN2(leaveTime + myLinkRecheckInterval, MIN2(veh->getBlockTime(), leaveTime) + recheck + 1), leaveTime + 1);
        }
        return leaveTime + myLinkRecheckInterval;
    }
    toSegment->updateEntryBlockTime(leaveTime);
    const SUMOTime entry = toSegment->hasSpaceFor(veh, leaveTime, qIdx);
    if (entry == leaveTime && (ignoreLink || veh->mayProceed())) {
        if (onSegment != nullptr) {
            if (veh->getQueIndex() == MESegment::PARKING_QUEUE) { // parking or just aborted parking
                if (veh->isParking()) {
                    veh->processStop();
                }
                veh->getEdge()->getLanes()[0]->removeParking(veh);  // TODO for GUI only
            } else if (myDeferred != nullptr && &onSegment->getEdge() != &toSegment->getEdge()) {
                // parallel loop: the source segment belongs to the edge of another thread, so the departure
                // is prepared here and completed on the main thread, which also applies the new event time
                DeferredOps::Send s;
                s.from = onSegment;
                s.veh = veh;
                s.qIdx = veh->getQueIndex();
                s.link = onSegment->getLink(veh);
                s.time = leaveTime;
                s.eventTime = -1;
                s.queued = veh->getQueuingTimeLoss() > 0;
                s.headway = onSegment->prepareSend(veh, toSegment, qIdx, leaveTime, MSMoveReminder::NOTIFICATION_JUNCTION);
                myDeferred->sends.push_back(s);
                toSegment->receive(veh, qIdx, leaveTime, false, ignoreLink, true, &myDeferred->sends.back().eventTime);
                return entry;
            } else {
                onSegment->send(veh, toSegment, qIdx, leaveTime, onSegment->getNextSegment() == nullptr ? MSMoveReminder::NOTIFICATION_JUNCTION : MSMoveReminder::NOTIFICATION_SEGMENT);
            }
            toSegment->receive(veh, qIdx, leaveTime, false, ignoreLink, &onSegment->getEdge() != &toSegment->getEdge());
        } else {
            WRITE_WARNINGF(TL("Vehicle '%' ends teleporting on edge '%':%, time=%."),
                           veh->getID(), toSegment->getEdge().getID(), toSegment->getIndex(), time2string(leaveTime));
            // this is not quite correct but suffices for interrogation by
            // subsequent methods (veh->getSpeed() needs segment != 0)
            veh->setSegment(myEdges2FirstSegments[veh->getEdge()->getNumericalID()]);
            // clean up detectors (do not add traffic data)
            // note: updateDatector is not called if leaveTime == getLastEntryTime()
            veh->updateDetectors(veh->getLastEntryTime(), veh->getEventTime(), true, MSMoveReminder::NOTIFICATION_TELEPORT);
            toSegment->receive(veh, qIdx, leaveTime, false, true, true);
        }
        return entry;
    } else {
        if (veh->getQueIndex() != MESegment::PARKING_QUEUE && veh->isStopped() && veh->endTriggeredStop()) {
            veh->processStop();
        }
    }
    if (entry == leaveTime && !ignoreLink) { // this is a long way of saying !veh->mayProceed() (which is a costly call)
        return entry + MAX2(SUMOTime(1), myLinkRecheckInterval);
    }
    return entry;
}


void
MELoop::checkCar(MEVehicle* veh) {
    const SUMOTime leaveTime = veh->getEventTime();
    MESegment* const onSegment = veh->getSegment();
    MESegment* const toSegment = veh->getQueIndex() == MESegment::PARKING_QUEUE ? onSegment : nextSegment(onSegment, veh);
    const bool teleporting = (onSegment == nullptr); // is the vehicle currently teleporting?
    // @note reason is only evaluated if toSegment == nullptr
    const SUMOTime nextEntry = changeSegment(veh, leaveTime, toSegment, MSMoveReminder::NOTIFICATION_ARRIVED, teleporting);
    if (nextEntry == leaveTime) {
        return;
    }
    const bool r1 = MSGlobals::gTimeToGridlock > 0 && veh->getWaitingTime() > MSGlobals::gTimeToGridlock;
    const bool r3 = MSGlobals::gTimeToTeleportDisconnected >= 0 && veh->getWaitingTime() > MSGlobals::gTimeToTeleportDisconnected;
    if (!veh->isStopped() && (r1 || r3)) {
        const bool disconnected = (MSGlobals::gTimeToTeleportDisconnected >= 0
                                   && veh->succEdge(1) != nullptr
                                   && veh->getEdge()->allowedLanes(*veh->succEdge(1), veh->getVClass()) == nullptr);
        if ((r1 && !disconnected) || (r3 && disconnected)) {
            teleportVehicle(veh, toSegment, disconnected);
            return;
        }
    }
    if (veh->getBlockTime() == SUMOTime_MAX && (!veh->isStopped()
            || (!veh->isStoppedTriggered() && veh->isStoppedParking()))) {
        veh->setBlockTime(leaveTime);
    }
    // receiving segment has recently received another vehicle or the junction is blocked
    SUMOTime newEventTime = nextEntry;
    if (nextEntry == SUMOTime_MAX) {
        // all usable queues on the next segment are full
        newEventTime = MAX3(toSegment->getEventTime() + 1, leaveTime + 1, leaveTime + myFullRecheckInterval);
        if (MSGlobals::gTimeToGridlock > 0) {
            // if teleporting is enabled, make sure we look at the vehicle when the gridlock-time is up
            const SUMOTime recheck = veh->getBlockTime() + 1 + MSGlobals::gTimeToGridlock;
            if (recheck > leaveTime) {
                newEventTime = MIN2(newEventTime, recheck);
            }
        }
        if (MSGlobals::gTimeToTeleportDisconnected >= 0) {
            const SUMOTime recheck = veh->getBlockTime() + 1 + MSGlobals::gTimeToTeleportDisconnected;
            if (recheck > leaveTime && veh->getEdge()->allowedLanes(*veh->succEdge(1), veh->getVClass()) == nullptr) {
                newEventTime = MIN2(newEventTime, recheck);
            }
        }
    }
    addLeaderCar(veh, teleporting ? nullptr : onSegment->getLink(veh), newEventTime);
}


void
MELoop::teleportVehicle(MEVehicle* veh, MESegment* const toSegment, bool disconnected) {
    const SUMOTime leaveTime = veh->getEventTime();
    MESegment* const onSegment = veh->getSegment();
    if (MSGlobals::gRemoveGridlocked) {
        WRITE_WARNINGF(TL("Teleporting vehicle '%'; waited too long, from edge '%':%, time=%."),
                       veh->getID(), onSegment->getEdge().getID(), onSegment->getIndex(),
                       time2string(leaveTime));
        MSNet::getInstance()->getVehicleControl().registerTeleportJam();
        int qIdx = 0;
        onSegment->send(veh, nullptr, qIdx, leaveTime, MSMoveReminder::NOTIFICATION_TELEPORT_ARRIVED);
        veh->setSegment(nullptr);
        MSNet::getInstance()->getVehicleControl().scheduleVehicleRemoval(veh);
        return;
    }
    const bool teleporting = (onSegment == nullptr); // is the vehicle already teleporting?
    // try to find a place on the current edge
    MESegment* teleSegment = disconnected ? toSegment : toSegment->getNextSegment();
    while (teleSegment != nullptr && changeSegment(veh, leaveTime, teleSegment, MSMoveReminder::NOTIFICATION_TELEPORT, true) != leaveTime) {
        // @caution the time to get to the next segment here is ignored XXX
        teleSegment = teleSegment->getNextSegment();
    }
    if (teleSegment != nullptr) {
        if (!teleporting) {
            // we managed to teleport in a single jump
            const std::string reason = disconnected ? " (disconnected)" : "";
            WRITE_WARNINGF(TL("Teleporting vehicle '%'; waited too long%, from edge '%':% to edge '%':%, time=%."),
                           veh->getID(), reason, onSegment->getEdge().getID(), onSegment->getIndex(),
                           teleSegment->getEdge().getID(), teleSegment->getIndex(), time2string(leaveTime));
            MSNet::getInstance()->getVehicleControl().registerTeleportJam();
        }
    } else {
        // teleport across the current edge and try insertion later
        if (!teleporting) {
            int qIdx = 0;
            // announce start of multi-step teleport, arrival will be announced in changeSegment()
            WRITE_WARNINGF(TL("Teleporting vehicle '%'; waited too long, from edge '%':%, time=%."),
                           veh->getID(), onSegment->getEdge().getID(), onSegment->getIndex(), time2string(leaveTime));
            MSNet::getInstance()->getVehicleControl().registerTeleportJam();
            // remove from current segment
            onSegment->send(veh, nullptr, qIdx, leaveTime, MSMoveReminder::NOTIFICATION_TELEPORT);
            // mark veh as teleporting
            veh->setSegment(nullptr);
        } else {
            veh->updateDetectors(veh->getLastEntryTime(), leaveTime, true, MSMoveReminder::NOTIFICATION_TELEPORT);
        }
        // @caution microsim uses current travel time teleport duration
        const SUMOTime teleArrival = leaveTime + TIME2STEPS(veh->getEdge()->getLength() / MAX2(veh->getEdge()->getSpeedLimit(), NUMERICAL_EPS));
        const bool atDest = veh->moveRoutePointer();
        if (atDest) {
            // teleporting to end of route
            changeSegment(veh, teleArrival, nullptr, MSMoveReminder::NOTIFICATION_TELEPORT_ARRIVED, true);
        } else {
            veh->setEventTime(teleArrival);
            addLeaderCar(veh, nullptr);
            // teleporting vehicles must react to rerouters
            getSegmentForEdge(*veh->getEdge())->addReminders(veh);
            veh->activateReminders(MSMoveReminder::NOTIFICATION_JUNCTION);
        }
    }
}


void
MELoop::addLeaderCar(MEVehicle* veh, MSLink* link, SUMOTime blockTime) {
    if (myDeferred != nullptr) {
        // parallel loop: other threads still read the current event time of this queue leader
        myDeferred->leaders.push_back(DeferredOps::Leader{veh, link, blockTime});
        return;
    }
    veh->setEventTime(MAX2(veh->getEventTime(), blockTime));
    myLeaderCars.push(LeaderEvent(veh));
    veh->setApproaching(link);
}


void
MELoop::segmentChanged(MESegment* s) {
    if (MSGlobals::gMesoParallelLoop && MSGlobals::gMesoLimitedJunctionControl) {
        if (myDeferred != nullptr) {
            myDeferred->changed.push_back(s);
        } else {
            myChanged.push_back(s);
        }
    }
}


void
MELoop::clearState() {
    myLeaderCars = LeaderEventQeue();
    myInvalidatedLeaderCars = LeaderEventQeue();
}


void
MELoop::removeLeaderCar(MEVehicle* v) {
    if (myDeferred != nullptr) {
        myDeferred->invalidated.push_back(std::make_pair(v, v->getEventTime()));
        return;
    }
    myInvalidatedLeaderCars.push(LeaderEvent(v));
}


void
MELoop::vaporizeCar(MEVehicle* v, MSMoveReminder::Notification reason) {
    int qIdx = 0;
    v->getSegment()->send(v, nullptr, qIdx, MSNet::getInstance()->getCurrentTimeStep(), reason);
    removeLeaderCar(v);
}


MESegment*
MELoop::nextSegment(MESegment* s, MEVehicle* v) {
    if (s != nullptr) { // vehicle is not teleporting
        MESegment* next = s->getNextSegment();
        if (next != nullptr) {
            // ok, the street continues
            return next;
        }
    }
    // we have to check the next edge in the vehicle's route
    const MSEdge* nextEdge = v->succEdge(1);
    if (nextEdge == nullptr) {
        // end of route
        return nullptr;
    }
    if (MSGlobals::gUsingInternalLanes && s != nullptr && s->getEdge().isNormal()) {
        const MSEdge* internal = s->getEdge().getInternalFollowingEdge(nextEdge, v->getVClass());
        if (internal) {
            nextEdge = internal;
        }
    }
    return myEdges2FirstSegments[nextEdge->getNumericalID()];
}


int
MELoop::numSegmentsFor(const double length, const double sLength) {
    int no = (int)floor(length / sLength + 0.5);
    if (no == 0) { // assure there is at least one segment
        return 1;
    } else {
        return no;
    }
}


void
MELoop::buildSegmentsFor(const MSEdge& e, const OptionsCont& oc) {
    const MESegment::MesoEdgeType& edgeType = MSNet::getInstance()->getMesoType(e.getEdgeType());
    const double length = e.getLength();
    const int numSegments = numSegmentsFor(length, edgeType.edgeLength);
    const double slength = length / (double)numSegments;
    MESegment* newSegment = nullptr;
    MESegment* nextSegment = nullptr;
    const bool laneQueue = oc.getBool("meso-lane-queue");
    const bool lift = oc.getBool("meso-ltm");
    bool multiQueue = laneQueue || (oc.getBool("meso-multi-queue") && e.getLanes().size() > 1 && e.getNumSuccessors() > 1);
    for (int s = numSegments - 1; s >= 0; s--) {
        std::string id = e.getID() + ":" + toString(s);
        newSegment = lift
                     ? new MELSegment(id, e, nextSegment, slength, e.getLanes()[0]->getSpeedLimit(), s, multiQueue, edgeType)
                     : new MESegment(id, e, nextSegment, slength, e.getLanes()[0]->getSpeedLimit(), s, multiQueue, edgeType);
        multiQueue = laneQueue;
        nextSegment = newSegment;
    }
    while (e.getNumericalID() >= static_cast<int>(myEdges2FirstSegments.size())) {
        myEdges2FirstSegments.push_back(0);
    }
    myEdges2FirstSegments[e.getNumericalID()] = newSegment;
    for (MSLane* lane : e.getLanes()) {
        lane->updateMesoGUISegments();
    }
}


MESegment*
MELoop::getSegmentForEdge(const MSEdge& e, double pos) {
    if (e.getNumericalID() >= (int)myEdges2FirstSegments.size()) {
        return nullptr;
    }
    MESegment* s = myEdges2FirstSegments[e.getNumericalID()];
    if (pos > 0) {
        double cpos = 0;
        while (s->getNextSegment() != nullptr && cpos + s->getLength() < pos) {
            cpos += s->getLength();
            s = s->getNextSegment();
        }
    }
    return s;
}


bool
MELoop::isEnteringRoundabout(const MSEdge& e) {
    for (const MSEdge* succ : e.getSuccessors()) {
        if (succ->isRoundabout()) {
            return true;
        }
    }
    return false;
}


MELoop::LeaderEvent::LeaderEvent(MEVehicle* v):
    time(v->getEventTime()),
    eventIndex(myEventCounter++),
    nid(v->getNumericalID()),
    veh(v) { }


// ===========================================================================
// parallel loop
// ===========================================================================
#ifdef HAVE_FOX
class MELoop::RoundTask : public MFXWorkerThread::Task {
public:
    RoundTask(MELoop& loop, const std::vector<std::pair<int, LeaderEvent> >& events, DeferredOps& ops)
        : myLoop(loop), myEvents(events), myOps(ops) {}

    void run(MFXWorkerThread* /*context*/) override {
        myDeferred = &myOps;
        try {
            for (const std::pair<int, LeaderEvent>& e : myEvents) {
                myLoop.checkCar(e.second.veh);
            }
        } catch (...) {
            myDeferred = nullptr;
            throw;
        }
        myDeferred = nullptr;
    }

private:
    MELoop& myLoop;
    const std::vector<std::pair<int, LeaderEvent> >& myEvents;
    DeferredOps& myOps;
};
#endif


bool
MELoop::isParallelSafe(const MEVehicle* veh, const MESegment* on, const MESegment* to) const {
    // the move may only read its own vehicle and write its target edge: no stop handling, nobody aboard,
    // no arrival on entering the edge, no teleport due and no reminder on either segment bound to the main thread
    if (veh->isStopped() || veh->hasStops() || veh->getPersonNumber() > 0 || veh->getContainerNumber() > 0) {
        return false;
    }
    if (&on->getEdge() != &to->getEdge() && veh->succEdge(2) == nullptr) {
        return false; // entering the last edge of the route
    }
    const SUMOTime waiting = veh->getWaitingTime();
    if ((MSGlobals::gTimeToGridlock > 0 && waiting > MSGlobals::gTimeToGridlock)
            || (MSGlobals::gTimeToTeleportDisconnected >= 0 && waiting > MSGlobals::gTimeToTeleportDisconnected)) {
        return false;
    }
    return !on->hasSerialReminders() && !to->hasSerialReminders();
}


void
MELoop::simulateParallel(SUMOTime tMax) {
#ifdef HAVE_FOX
    MFXWorkerThread::Pool& pool = MSNet::getInstance()->getEdgeControl().getThreadPool();
    const int numThreads = MSGlobals::gNumSimThreads;
    std::vector<DeferredOps> ops(numThreads);
    std::vector<LeaderEvent> serial;
    std::vector<std::vector<std::pair<int, LeaderEvent> > > parallel(numThreads); // per thread: target edge, event
    // a round runs everything that is due; what it reschedules within the step is due again in the next round
    while (!myLeaderCars.empty() && myLeaderCars.top().time <= tMax) {
        serial.clear();
        for (std::vector<std::pair<int, LeaderEvent> >& p : parallel) {
            p.clear();
        }
        int numParallel = 0;
        while (!myLeaderCars.empty() && myLeaderCars.top().time <= tMax) {
            const LeaderEvent e = myLeaderCars.top();
            myLeaderCars.pop();
            if (skipInvalidated(e)) {
                continue;
            }
            MESegment* const on = e.veh->getSegment();
            MESegment* const to = on == nullptr || e.veh->getQueIndex() == MESegment::PARKING_QUEUE ? nullptr : nextSegment(on, e.veh);
            if (to != nullptr && isParallelSafe(e.veh, on, to)) {
                // all moves onto an edge run on the thread of its rng (as the lanes in MSEdgeControl)
                const MSEdge& edge = to->getEdge();
                parallel[edge.getLanes()[0]->getRNGIndex() % numThreads].push_back(std::make_pair(edge.getNumericalID(), e));
                numParallel++;
            } else {
                serial.push_back(e);
            }
        }
        if (numParallel < MIN_PARALLEL_MOVES) {
            // the small rounds within a step are not worth the threads (the limit does not depend on their number)
            for (const std::vector<std::pair<int, LeaderEvent> >& p : parallel) {
                for (const std::pair<int, LeaderEvent>& e : p) {
                    serial.push_back(e.second);
                }
            }
            std::sort(serial.begin(), serial.end(), [](const LeaderEvent& a, const LeaderEvent& b) {
                return b < a;
            });
            numParallel = 0;
        }
        for (MESegment* const s : myChanged) {
            s->commitOccupancy();
        }
        myChanged.clear();
        // 1. the moves that only touch their target edge run on the workers, edge by edge in time order
        for (int t = 0; t < numThreads && numParallel > 0; t++) {
            std::sort(parallel[t].begin(), parallel[t].end(), [](const std::pair<int, LeaderEvent>& a, const std::pair<int, LeaderEvent>& b) {
                return a.first != b.first ? a.first < b.first : b.second < a.second;
            });
            pool.add(new RoundTask(*this, parallel[t], ops[t]), t);
        }
        pool.waitAll();
        // 2. apply what the workers recorded in an order that does not depend on the threads
        std::vector<DeferredOps::Send> sends;
        std::vector<DeferredOps::Leader> leaders;
        for (DeferredOps& o : ops) {
            sends.insert(sends.end(), o.sends.begin(), o.sends.end());
            leaders.insert(leaders.end(), o.leaders.begin(), o.leaders.end());
            for (const std::pair<MEVehicle*, SUMOTime>& inv : o.invalidated) {
                LeaderEvent le(inv.first);
                le.time = inv.second;
                myInvalidatedLeaderCars.push(le);
            }
            myChanged.insert(myChanged.end(), o.changed.begin(), o.changed.end());
            o.clear();
        }
        std::sort(sends.begin(), sends.end(), [](const DeferredOps::Send& a, const DeferredOps::Send& b) {
            return a.time != b.time ? a.time < b.time : a.veh->getNumericalID() < b.veh->getNumericalID();
        });
        for (const DeferredOps::Send& s : sends) {
            // the event times of the arrivals come first: a departure may promote an arrived vehicle to queue leader
            s.veh->setEventTime(s.eventTime);
            s.veh->setUnqueuedEventTime(s.eventTime);
        }
        for (const DeferredOps::Send& s : sends) {
            s.from->completeSend(s.veh, s.qIdx, s.link, s.time, s.headway, s.queued);
        }
        for (DeferredOps::Leader& l : leaders) {
            l.eventTime = MAX2(l.eventTime, l.veh->getEventTime());
        }
        std::sort(leaders.begin(), leaders.end(), [](const DeferredOps::Leader& a, const DeferredOps::Leader& b) {
            return a.eventTime != b.eventTime ? a.eventTime < b.eventTime : a.veh->getNumericalID() < b.veh->getNumericalID();
        });
        for (const DeferredOps::Leader& l : leaders) {
            l.veh->setEventTime(l.eventTime);
            myLeaderCars.push(LeaderEvent(l.veh));
            l.veh->setApproaching(l.link);
        }
        // 3. the remaining events run here in time order
        for (const LeaderEvent& e : serial) {
            if (!skipInvalidated(e)) {
                checkCar(e.veh);
            }
        }
    }
#else
    UNUSED_PARAMETER(tMax);
#endif
}

/****************************************************************************/
