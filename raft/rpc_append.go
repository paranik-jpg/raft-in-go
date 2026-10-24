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
type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself if obsolete.
	Success bool // True if follower contained entry matching PrevLogIndex and PrevLogTerm.
}

// AppendEntries is the incoming RPC handler invoked by the leader.
// Handles Outcome 2 for candidates/followers:
// "If an incoming AppendEntries RPC is received from a peer claiming a term >= candidate's currentTerm,
// recognize leader, step down to Follower, and reset election timer."
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Default response: reject
	reply.Success = false

	// Rule 1: Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Rule 2: If term >= currentTerm, recognize valid leader.
	// If term > currentTerm, or if we were Candidate/Leader, step down to Follower (§5.2).
	if args.Term > rf.currentTerm || rf.role != RoleFollower {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// Signal the event loop that a valid heartbeat was received from a legitimate leader.
	// This resets the election timer in the run() loop.
	sendSignal(rf.heartbeatCh)
	reply.Success = true
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

	for peer := 0; peer < peersCount; peer++ {
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
			}
		}(peer)
	}
}

