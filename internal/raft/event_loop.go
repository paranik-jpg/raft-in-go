package raft

import (
	"math/rand"
	"time"
)

// =============================================================================
// MAIN EVENT-DRIVEN LIFECYCLE RUNNER
// =============================================================================

// run is the centralized event loop of a Raft node.
// It constructs a continuous select loop over three primary drivers:
//  1. electionTimer.C: Triggers conversion from Follower to Candidate, or
//     restarts Candidate election on split-vote timeouts.
//  2. heartbeatTimer.C: (Active only when Leader) Triggers parallel
//     AppendEntries dispatches to all peers.
//  3. stopCh: Internal lifecycle termination channel.
func (rf *Raft) run() {
	electionTimer := time.NewTimer(rf.getElectionTimeout())
	defer electionTimer.Stop()

	heartbeatTimer := time.NewTimer(HeartbeatInterval)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-rf.stopCh:
			// 1. Internal lifecycle termination channel
			return

		case <-electionTimer.C:
			// 2. electionTimer.C: Triggers conversion from Follower to Candidate,
			//    or restarts Candidate election on split-vote timeouts.
			rf.mu.Lock()
			// Leaders never transition to candidates due to election timeouts.
			if rf.role != RoleLeader {
				rf.becomeCandidate()
				rf.startElection()
			}
			rf.mu.Unlock()

			// Reset election timer for next potential timeout
			resetTimer(electionTimer, rf.getElectionTimeout())

		case <-heartbeatTimer.C:
			// 3. heartbeatTimer.C: (Active only when Leader) Triggers parallel
			//    AppendEntries dispatches to all peers.
			rf.mu.Lock()
			isLeader := (rf.role == RoleLeader)
			rf.mu.Unlock()

			if isLeader {
				rf.sendHeartbeats()
			}

			// Always re-arm heartbeatTimer to fire at HeartbeatInterval
			resetTimer(heartbeatTimer, HeartbeatInterval)

		case <-rf.heartbeatCh:
			// Reset election timer when receiving valid heartbeat from Leader
			resetTimer(electionTimer, rf.getElectionTimeout())

		case <-rf.grantVoteCh:
			// Reset election timer when granting vote to a Candidate
			resetTimer(electionTimer, rf.getElectionTimeout())

		case <-rf.winElectionCh:
			// Candidate collected majority quorum -> transitioned to Leader
			rf.mu.Lock()
			if rf.role == RoleCandidate {
				rf.becomeLeader()
				resetTimer(heartbeatTimer, HeartbeatInterval)
				rf.mu.Unlock()

				// Send immediate initial heartbeats upon winning election outside rf.mu!
				rf.sendHeartbeats()
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

// resetTimer safely stops, drains (if necessary), and resets a time.Timer.
// This prevents spurious stale triggers when reusing timers in Go's runtime.
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

// getElectionTimeout returns a randomized duration in [MinElectionTimeout, MaxElectionTimeout).
// Randomization breaks symmetry between nodes, preventing split-vote deadlocks.
func (rf *Raft) getElectionTimeout() time.Duration {
	delta := int64(MaxElectionTimeout - MinElectionTimeout)
	return MinElectionTimeout + time.Duration(rand.Int63n(delta))
}

