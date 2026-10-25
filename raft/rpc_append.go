package raft

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
	if args.PrevLogIndex > lastLogIndex {
		reply.ConflictTerm = -1
		// The leader jumps straight to where our log ends.
		reply.ConflictIndex = len(rf.log) // First missing index
		return
	}

	// Case 3b: Receiver has entry at PrevLogIndex, but term mismatches
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// Scan backward to find the earliest index for this conflicting term
		// Find the very first index of that conflicting term
		for idx := args.PrevLogIndex; idx >= 1; idx-- {
			if rf.log[idx].Term == reply.ConflictTerm {
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
		// Below cond. => rf has something ahead!
		if entryIndex <= rf.getLastLogIndex() {
			if rf.log[entryIndex].Term != entry.Term {
				// Conflict detected: truncate log and append remaining entries
				// rf.log[:entryIndex] => Keep index just before entryIndex.
				rf.log = append(rf.log[:entryIndex], args.Entries[i:]...)
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
		if rf.log[N].Term != rf.currentTerm {
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
			break
		}
	}
}

// sendAppendEntries dispatches an AppendEntries RPC to the specified peer.
func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if peer < 0 || peer >= len(rf.peers) || rf.peers[peer] == nil {
		return false
	}
	rf.peers[peer].AppendEntries(args, reply)
	return true
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
			prevLogIndex := rf.nextIndex[p] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			rf.mu.Unlock()

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil, // Empty slice for heartbeat
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
				if reply.ConflictTerm == -1 {
					// Follower log too short: jump directly to follower's log length
					rf.nextIndex[p] = reply.ConflictIndex
				} else {
					// Term mismatch: search if leader has an entry with ConflictTerm
					foundTerm := false
					for idx := rf.getLastLogIndex(); idx >= 1; idx-- {
						if rf.log[idx].Term == reply.ConflictTerm {
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
