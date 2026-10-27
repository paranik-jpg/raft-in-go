package raft

import (
	"sync"
	"time"
)

// AppendEntriesArgs defines the arguments sent by the leader for heartbeats and replication.
// Corresponds strictly to Raft §5.2 and §5.3.
type AppendEntriesArgs struct {
	Term         int        // Leader's current term.
	LeaderID     int        // Identifier of leader so follower can redirect clients.
	PrevLogIndex int        // Index of log entry immediately preceding new ones.
	PrevLogTerm  int        // Term of PrevLogIndex entry.
	Entries      []LogEntry // Log entries to store (empty for heartbeat).
	LeaderCommit int        // Leader's commitIndex.
}

// AppendEntriesReply defines the response sent back by peers.
// Includes ConflictIndex and ConflictTerm for Raft fast-recovery log backtracking (§5.3).
type AppendEntriesReply struct {
	Term          int  // CurrentTerm, for leader to update itself if obsolete.
	Success       bool // True if follower contained entry matching PrevLogIndex and PrevLogTerm.
	ConflictIndex int  // First index of the conflicting term (or log length if too short).
	ConflictTerm  int  // Term of the conflicting entry (-1 if follower log is too short).
}

// AppendEntries is the incoming RPC handler invoked by the leader.
// Implements the complete Follower Receiver Execution Logic (§5.3):
// 1. Term Rejection
// 2. Leader Recognition & Timer Reset
// 3. Log Matching Check with Fast Recovery Diagnostics
// 4. Conflict Truncation & Splicing
// 5. Advancing Commit Index
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Default response: reject
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0

	// Rule 1 (Term Rejection): If args.Term < currentTerm, reject immediately.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Rule 2 (Leader Recognition): If args.Term >= currentTerm, update term,
	// demote to Follower if candidate/leader, and reset election timer.
	if args.Term > rf.currentTerm || rf.role != RoleFollower {
		rf.becomeFollower(args.Term)
	}

	// Always report our currentTerm back to the leader
	reply.Term = rf.currentTerm

	// Valid leader recognized -> reset local election timer
	sendSignal(rf.heartbeatCh)

	// Rule 3 (Log Matching Check):
	// Check if receiver's log contains an entry at args.PrevLogIndex matching args.PrevLogTerm.
	// If no entry exists or term mismatches: return Success = false with fast recovery fields.
	lastLogIndex := rf.getLastLogIndex()

	// Case 3a: Receiver's log is too short (does not contain PrevLogIndex)
	// ConflictIndex = lastLogIndex + 1 (len of log in 1-based indexing)
	// ConflictTerm = 0 (no term present)
	if args.PrevLogIndex > lastLogIndex {
		reply.ConflictTerm = 0
		// "The follower's log ended at lastLogIndex.
		// So the very first missing index where the follower needs data is lastLogIndex + 1!"
		reply.ConflictIndex = lastLogIndex + 1
		return
	}

	// Case 3b: Receiver has entry at PrevLogIndex, but term mismatches
	// Check if PrevLogIndex is behind our compacted log base
	if args.PrevLogIndex < rf.getFirstLogIndex() {
		// Follower has already compacted past PrevLogIndex, meaning this entry was already committed
		reply.ConflictTerm = 0
		reply.ConflictIndex = rf.getFirstLogIndex() + 1
		return
	}

	prevSliceIdx := rf.toSliceIndex(args.PrevLogIndex)
	if rf.log[prevSliceIdx].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[prevSliceIdx].Term
		// Scan backward to find the earliest index for this conflicting term
		baseIdx := rf.getFirstLogIndex()
		// idx > baseIdx => Never run off the cliff into discarded memory!
		for idx := args.PrevLogIndex; idx > baseIdx; idx-- {
			sIdx := rf.toSliceIndex(idx)
			if rf.log[sIdx].Term == reply.ConflictTerm {
				reply.ConflictIndex = idx
			} else {
				break
			}
		}
		return
	}

	// Rule 4 (Conflict Truncation & Splicing):
	// If an existing entry conflicts with a new entry (same index but different term):
	// truncate the local log at that index, dropping the conflicting entry and all that follow it.
	// Append any new entries not already in the log.
	for i, entry := range args.Entries {
		// EntryIndex => Index to start the matching from.
		entryIndex := args.PrevLogIndex + 1 + i
		if entryIndex <= rf.getFirstLogIndex() {
			// Already snapshotted on follower, skip
			continue
		}
		if entryIndex <= rf.getLastLogIndex() {
			sIdx := rf.toSliceIndex(entryIndex)
			if rf.log[sIdx].Term != entry.Term {
				// Conflict detected: truncate log and append remaining entries
				rf.log = append(rf.log[:sIdx], args.Entries[i:]...)
				break
			}
			// Identical entry already in log; do not duplicate
		} else {
			// Beyond existing log length: append remaining entries
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	// Rule 5 (Advancing Commit Index):
	// If args.LeaderCommit > commitIndex, advance follower commit:
	// commitIndex = min(args.LeaderCommit, index of last new entry)
	// Leader might have committed more b/w Rule 4 and Rule 5
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		// Wake up applyLoop to deliver newly committed entries to state machine
		if rf.applyCond != nil {
			// Instantly wake up applyLoop
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
}

// checkAndUpdateCommitIndex searches for an index N > commitIndex such that a
// majority of matchIndex[i] >= N, and log[N].Term == currentTerm (§5.3, §5.4.2).
// Must be called with rf.mu locked.
func (rf *Raft) checkAndUpdateCommitIndex() {
	if rf.role != RoleLeader {
		return
	}

	quorum := (rf.peersCount / 2) + 1
	lastLogIndex := rf.getLastLogIndex()

	// Search downwards from lastLogIndex to commitIndex + 1
	for N := lastLogIndex; N > rf.commitIndex; N-- {
		// The Raft Safety Invariant on Leader Commits (§5.4.2):
		// A leader cannot commit a log entry from an earlier term simply by counting replicas;
		// it must commit an entry from its current term by counting replicas,
		// which indirectly commits earlier entries.
		sliceN := rf.toSliceIndex(N)
		if rf.log[sliceN].Term != rf.currentTerm {
			continue
		}

		// Count replicas that have stored entry N
		// The leader itself always has entry N
		count := 1
		for peer := 0; peer < rf.peersCount; peer++ {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= N {
				count++
			}
		}

		// If stored on a majority quorum, advance commitIndex to N
		if count >= quorum {
			rf.commitIndex = N
			// Wake up applyLoop to deliver newly committed entries to state machine
			if rf.applyCond != nil {
				// Instantly wake up applyLoop
				rf.applyCond.Broadcast()
			}
			break
		}
	}
}

// sendAppendEntries dispatches an AppendEntries RPC to the specified peer.
// If a NetworkMatrix is configured, it routes through the simulator switch.
func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	netMatrix := rf.netMatrix
	from := rf.me
	if peer < 0 || peer >= len(rf.peers) || rf.peers[peer] == nil {
		rf.mu.Unlock()
		return false
	}
	targetPeer := rf.peers[peer]
	rf.mu.Unlock()

	if netMatrix != nil {
		return netMatrix.CallAppendEntries(from, peer, args, reply)
	}

	targetPeer.AppendEntries(args, reply)
	return true
}

// sendHeartbeatRound sends an immediate round of empty AppendEntries heartbeats to all peers
// and blocks until either a majority confirms or it fails to achieve quorum.
// Used by the ReadIndex protocol (§8) to verify leadership authority before serving reads.
func (rf *Raft) sendHeartbeatRound() bool {
	rf.mu.Lock()
	if rf.role != RoleLeader {
		rf.mu.Unlock()
		return false
	}
	term := rf.currentTerm
	leaderID := rf.me
	leaderCommit := rf.commitIndex
	peersCount := rf.peersCount
	quorum := (peersCount / 2) + 1
	rf.mu.Unlock()

	// If single-node cluster, we already constitute a majority
	if peersCount == 1 {
		return true
	}

	ackCount := 1 // Leader itself
	var ackMu sync.Mutex
	doneCh := make(chan bool, 1)

	for peer := range peersCount {
		if peer == leaderID {
			continue
		}

		go func(p int) {
			rf.mu.Lock()
			if rf.role != RoleLeader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[p] - 1
			prevSliceIdx := rf.toSliceIndex(prevLogIndex)
			prevLogTerm := rf.log[prevSliceIdx].Term
			rf.mu.Unlock()

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil, // Zero entries
				LeaderCommit: leaderCommit,
			}
			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(p, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != RoleLeader || rf.currentTerm != term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				ackMu.Lock()
				ackCount++
				if ackCount == quorum {
					select {
					case doneCh <- true:
					default:
					}
				}
				ackMu.Unlock()
			}
		}(peer)
	}

	// Wait for majority confirmation with a reasonable timeout
	select {
	case <-doneCh:
		return true
	case <-time.After(150 * time.Millisecond):
		return false
	case <-rf.stopCh:
		return false
	}
}

// ReadIndex executes the Raft Linearizable Read protocol (§8):
// 1. Capture current commitIndex as readIndex.
// 2. Send immediate heartbeat round to all peers and await majority confirmation.
// 3. Once majority confirms leadership, wait until lastApplied >= readIndex.
// 4. Returns readIndex and true if successful; (0, false) if not leader or partitioned.
func (rf *Raft) ReadIndex() (int, bool) {
	rf.mu.Lock()
	if rf.role != RoleLeader {
		rf.mu.Unlock()
		return 0, false
	}
	// Step 1: Capture current commitIndex as readIndex
	readIndex := rf.commitIndex
	rf.mu.Unlock()

	// Step 2: Send immediate heartbeat round and await majority confirmation
	if !rf.sendHeartbeatRound() {
		return 0, false
	}

	// Step 3: Wait until lastApplied >= readIndex
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Verify we are still leader in the same term
	if rf.role != RoleLeader {
		return 0, false
	}

	for rf.lastApplied < readIndex {
		select {
		case <-rf.stopCh:
			return 0, false
		default:
		}
		rf.applyCond.Wait()
	}

	// Step 4: Ready to read from state machine with linearizability guaranteed
	return readIndex, true
}

// sendHeartbeats broadcasts an immediate empty AppendEntries RPC to all peers.
// Suppresses competing candidates and asserts leadership.
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.role != RoleLeader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	leaderID := rf.me
	leaderCommit := rf.commitIndex
	peersCount := rf.peersCount
	rf.mu.Unlock()

	for peer := range peersCount {
		if peer == leaderID {
			continue
		}

		go func(p int) {
			rf.mu.Lock()
			if rf.role != RoleLeader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			// If follower's nextIndex has fallen behind our earliest compacted entry (log[0].Index),
			// the leader cannot send AppendEntries because those entries were already truncated.
			// In accordance with Raft §7, leader must send an InstallSnapshot RPC instead!
			if rf.nextIndex[p] <= rf.getFirstLogIndex() {
				rf.sendInstallSnapshotToPeer(p)
				return
			}

			prevLogIndex := rf.nextIndex[p] - 1
			prevSliceIdx := rf.toSliceIndex(prevLogIndex)
			prevLogTerm := rf.log[prevSliceIdx].Term

			// Package any log entries from nextIndex[p] onwards
			var entries []LogEntry
			if rf.getLastLogIndex() >= rf.nextIndex[p] {
				// The leader has new log entries that peer p has not received yet!
				startSliceIdx := rf.toSliceIndex(rf.nextIndex[p])
				entries = make([]LogEntry, len(rf.log[startSliceIdx:]))
				copy(entries, rf.log[startSliceIdx:])
			}
			rf.mu.Unlock()

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries, // Entries to replicate (nil/empty if pure heartbeat)
				LeaderCommit: leaderCommit,
			}
			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(p, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Check for stale response
			if rf.role != RoleLeader || rf.currentTerm != term {
				return
			}

			// If peer has a higher term, step down immediately
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			// Handle AppendEntries response:
			if reply.Success {
				// After receiving successful AppendEntries response:
				// Update matchIndex[peer] and nextIndex[peer]
				match := prevLogIndex + len(args.Entries)
				if match > rf.matchIndex[p] {
					rf.matchIndex[p] = match
				}
				rf.nextIndex[p] = rf.matchIndex[p] + 1

				// Check if this new replication allows advancing commitIndex (§5.3, §5.4.2)
				rf.checkAndUpdateCommitIndex()
			} else {
				// Consistency check failed: use fast recovery hints to update nextIndex
				if reply.ConflictTerm == 0 {
					// Follower log too short: jump directly to follower's log length + 1
					rf.nextIndex[p] = reply.ConflictIndex
				} else {
					// Term mismatch: search if leader has an entry with ConflictTerm
					foundTerm := false
					baseIdx := rf.getFirstLogIndex()
					for idx := rf.getLastLogIndex(); idx > baseIdx; idx-- {
						sIdx := rf.toSliceIndex(idx)
						if rf.log[sIdx].Term == reply.ConflictTerm {
							rf.nextIndex[p] = idx + 1
							foundTerm = true
							break
						}
					}
					if !foundTerm {
						// Leader does not have ConflictTerm: jump to first index of conflicting term
						rf.nextIndex[p] = reply.ConflictIndex
					}
				}
				// Ensure nextIndex never falls below 1
				if rf.nextIndex[p] < 1 {
					rf.nextIndex[p] = 1
				}
			}
		}(peer)
	}
}
