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
/// @file    MELoop.h
/// @author  Daniel Krajzewicz
/// @date    Tue, May 2005
///
// The main mesocopic simulation loop
/****************************************************************************/
#pragma once
#include <config.h>

#include <vector>
#include <queue>
#include <map>
#include <utils/common/SUMOTime.h>
#include <microsim/MSMoveReminder.h>


// ===========================================================================
// class declarations
// ===========================================================================
class MESegment;
class MEVehicle;
class MSEdge;
class MSLink;
class MSVehicleControl;
class OptionsCont;

// ===========================================================================
// class definitions
// ===========================================================================
/**
 * @class MELoop
 * @brief The main mesocopic simulation loop
 */
class MELoop {
public:
    /// SUMO constructor
    MELoop(const SUMOTime recheckInterval);

    ~MELoop();

    /** @brief Perform simulation up to the given time
     *
     * Checks all vehicles with an event time less or equal than the given time.
     *
     * @param[in] tMax the end time for the sim step
     */
    void simulate(SUMOTime tMax);

    /// @brief whether the calling thread runs moves of the parallel loop (see simulateParallel)
    static bool inParallelPhase() {
        return myDeferred != nullptr;
    }

    /// @brief notes that the occupancy of the segment changed (the parallel loop keeps committed copies for junction control)
    void segmentChanged(MESegment* s);

    /** @brief Adds the given car to the leading vehicles
     *
     * @param[in] veh the car which became a leading one
     * @param[in] link the link on which the car shall register its approach
     * @param[in] blockTime the earliest event time of the car (-1: its current event time)
     */
    void addLeaderCar(MEVehicle* veh, MSLink* link, SUMOTime blockTime = -1);

    /** @brief Removes the given car from the leading vehicles
     *
     * @param[in] v the car which was a leading one
     */
    void removeLeaderCar(MEVehicle* v);

    /** @brief remove the given car and clean up the relevant data structures */
    void vaporizeCar(MEVehicle* v, MSMoveReminder::Notification reason);

    /// @brief whether the given edge is entering a roundabout
    static bool isEnteringRoundabout(const MSEdge& e);

    /** @brief Compute number of segments per edge (best value stay close to the configured segment length) */
    static int numSegmentsFor(const double length, const double slength);

    /** @brief Build the segments for a given edge
     *
     * @param[in] e the edge to build for
     */
    void buildSegmentsFor(const MSEdge& e, const OptionsCont& oc);

    /** @brief Get the segment for a given edge at a given position
     *
     * @param[in] e the edge to get the segment for
     * @param[in] pos the position to get the segment for
     * @return The relevant segment
     */
    MESegment* getSegmentForEdge(const MSEdge& e, double pos = 0);

    /** @brief change to the next segment
     * this handles combinations of the following cases:
     * (ending / continuing route) and (leaving segment / finishing teleport)
     */
    SUMOTime changeSegment(MEVehicle* veh, SUMOTime leaveTime, MESegment* const toSegment,
                           MSMoveReminder::Notification reason, const bool ignoreLink = false) const;

    /** @brief Remove all vehicles before quick-loading state */
    void clearState();

private:
    /** @brief Check whether the vehicle may move
     *
     * This method is called when the vehicle reaches its event time and checks
     *  whether it may proceed to the next segment.
     *
     * @param[in] veh The vehicle to check
     */
    void checkCar(MEVehicle* veh);

    /** @brief Retrieve next segment
     *
     * If the segment is not the last on the current edge, its successor is returned.
     *  Otherwise, the first segment of the edge at which the vehicle continues
     *  his journey is returned.
     *
     * @param[in] s The segment the vehicle is currently at
     * @param[in] v The vehicle to get the next segment for
     * @return The vehicle's next segment
     * @todo Recheck the "quick and dirty" stuff (@see MESegment::saveState, @see MESegment::loadState)
     */
    MESegment* nextSegment(MESegment* s, MEVehicle* v);


    /** @brief teleports a vehicle or continues a teleport
     * @param[in] veh The vehicle to teleport
     * @param[in] toSegment The first segment where the vehicle may reenter the network
     */
    void teleportVehicle(MEVehicle* veh, MESegment* const toSegment, bool disconnected);

    /** @brief Perform simulation up to the given time on several threads (option --threads with --meso-ltm)
     *
     * Runs rounds until no event is due: the moves that only touch their own vehicle and their target
     * edge run on the worker threads (all moves onto an edge on the thread of its rng, as in MSEdgeControl),
     * which record their side effects on shared state (departures from segments of other edges, event
     * scheduling, link registrations) instead of applying them; the main thread then applies those in a
     * canonical order and runs the remaining events (stops, arrivals, teleports, segments with reminders
     * bound to one thread). Junction control ignores foes due before the checked vehicle (MSLink::blockedAtTime).
     * Events of different edges may run out of time order within a round, so the results differ slightly
     * from the serial loop but not between thread counts (the small rounds that run on the main thread
     * are chosen by a fixed limit).
     *
     * @param[in] tMax the end time for the sim step
     */
    void simulateParallel(SUMOTime tMax);

    /// @brief whether the move of the vehicle from its segment to the given one may run on a worker thread
    bool isParallelSafe(const MEVehicle* veh, const MESegment* on, const MESegment* to) const;

private:

    struct LeaderEvent {
        LeaderEvent(MEVehicle* v);

        SUMOTime time;
        // eventIndex ensures stable sorting of vehicles with the same event time
        long long int eventIndex;
        // nid allows safely comparing events for deleted vehicles (myInvalidatedLeaderCars)
        long long int nid;
        MEVehicle* veh;

        bool operator<(const LeaderEvent& b) const {
            if (time == b.time) {
                return eventIndex > b.eventIndex;
            }
            return time > b.time;
        }

        bool operator==(const LeaderEvent& b) const {
            return time == b.time && nid == b.nid;
        }

        static long long int myEventCounter;
    };

    /// @brief check whether the current car was invalidated and remove it from myInvalidatedLeaderCars
    bool wasInvalidated(const LeaderEvent& e);

    /// @brief drops the invalidations older than the event and tells whether the event itself was invalidated
    bool skipInvalidated(const LeaderEvent& e);

    /// @brief what a worker thread of the parallel loop records instead of changing shared state
    struct DeferredOps {
        /// @brief a departure from a segment of another edge, completed on the main thread (MESegment::completeSend)
        struct Send {
            MESegment* from;
            MEVehicle* veh;
            int qIdx;
            MSLink* link;
            SUMOTime time;
            SUMOTime headway;
            SUMOTime eventTime;
            bool queued;
        };
        std::vector<Send> sends;
        /// @brief an event to schedule: the vehicle, the link it registers on and its earliest event time (-1: the current one)
        struct Leader {
            MEVehicle* veh;
            MSLink* link;
            SUMOTime eventTime;
        };
        std::vector<Leader> leaders;
        /// @brief removeLeaderCar calls: the vehicle and the time of the event to invalidate
        std::vector<std::pair<MEVehicle*, SUMOTime> > invalidated;
        /// @brief segments whose occupancy changed
        std::vector<MESegment*> changed;

        void clear() {
            sends.clear();
            leaders.clear();
            invalidated.clear();
            changed.clear();
        }
    };

    /// @brief runs the moves of one thread of a round
    class RoundTask;

    /// @brief the buffer of the worker thread while it runs moves of the parallel loop, nullptr otherwise
    static thread_local DeferredOps* myDeferred;

    /// @brief the number of moves below which a round runs on the main thread
    static const int MIN_PARALLEL_MOVES;

    /// @brief segments whose occupancy changed on the main thread since the last commit of the parallel loop
    std::vector<MESegment*> myChanged;


    /// @brief leader cars in the segments sorted by exit time
    typedef std::priority_queue<LeaderEvent> LeaderEventQeue;
    LeaderEventQeue myLeaderCars;
    LeaderEventQeue myInvalidatedLeaderCars;

    /// @brief mapping from internal edge ids to their initial segments
    std::vector<MESegment*> myEdges2FirstSegments;

    /// @brief the interval at which to recheck at full segments (<=0 means asap)
    const SUMOTime myFullRecheckInterval;

    /// @brief the interval at which to recheck at blocked junctions (<=0 means asap)
    const SUMOTime myLinkRecheckInterval;

private:
    /// @brief Invalidated copy constructor.
    MELoop(const MELoop&);

    /// @brief Invalidated assignment operator.
    MELoop& operator=(const MELoop&);
};
