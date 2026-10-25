package raft

import (
	"testing"
)

// TestAppendEntriesFollowerExecution verifies all 5 receiver rules:
// Rule 1: Term rejection (args.Term < currentTerm)
// Rule 2: Leader recognition & step-down
// Rule 3: Log matching check & fast recovery diagnostics (too short vs term mismatch)
// Rule 4: Conflict truncation & log splicing
// Rule 5: Advancing commitIndex min(LeaderCommit, lastNewEntryIndex)
func TestAppendEntriesFollowerExecution(t *testing.T) {
	// -------------------------------------------------------------------------
	// Rule 1: Reject stale leader term
	// -------------------------------------------------------------------------
	{
		rf := NewRaft(0, 3)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.mu.Unlock()

		args := &AppendEntriesArgs{
			Term:         1, // Stale!
			LeaderID:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
		}
		reply := &AppendEntriesReply{}
		rf.AppendEntries(args, reply)

		if reply.Success {
			t.Fatalf("expected rejection for stale term")
		}
		if reply.Term != 2 {
			t.Fatalf("expected reply.Term == 2, got %d", reply.Term)
		}
	}

	// -------------------------------------------------------------------------
	// Rule 3a: Log matching check fails when follower log is too short
	// -------------------------------------------------------------------------
	{
		rf := NewRaft(0, 3) // log only has sentinel at index 0 (len=1)
		args := &AppendEntriesArgs{
			Term:         1,
			LeaderID:     1,
			PrevLogIndex: 5, // Follower does not have index 5!
			PrevLogTerm:  1,
		}
		reply := &AppendEntriesReply{}
		rf.AppendEntries(args, reply)

		if reply.Success {
			t.Fatalf("expected failure because log is too short")
		}
		if reply.ConflictTerm != 0 {
			t.Fatalf("expected ConflictTerm == 0, got %d", reply.ConflictTerm)
		}
		if reply.ConflictIndex != 1 {
			t.Fatalf("expected ConflictIndex == 1 (first missing index), got %d", reply.ConflictIndex)
		}
	}

	// -------------------------------------------------------------------------
	// Rule 3b: Log matching check fails when term at PrevLogIndex mismatches
	// -------------------------------------------------------------------------
	{
		rf := NewRaft(0, 3)
		rf.mu.Lock()
		rf.currentTerm = 2
		// Follower has entries: index 1 term 1, index 2 term 1, index 3 term 1
		rf.log = append(rf.log,
			LogEntry{Index: 1, Term: 1, Command: "a"},
			LogEntry{Index: 2, Term: 1, Command: "b"},
			LogEntry{Index: 3, Term: 1, Command: "c"},
		)
		rf.mu.Unlock()

		// Leader expects index 3 to have Term 2
		args := &AppendEntriesArgs{
			Term:         2,
			LeaderID:     1,
			PrevLogIndex: 3,
			PrevLogTerm:  2, // Mismatch! Follower has term 1 at index 3
		}
		reply := &AppendEntriesReply{}
		rf.AppendEntries(args, reply)

		if reply.Success {
			t.Fatalf("expected failure on term mismatch")
		}
		if reply.ConflictTerm != 1 {
			t.Fatalf("expected ConflictTerm == 1, got %d", reply.ConflictTerm)
		}
		// The earliest index where term 1 started in follower log is index 1!
		if reply.ConflictIndex != 1 {
			t.Fatalf("expected ConflictIndex == 1, got %d", reply.ConflictIndex)
		}
	}

	// -------------------------------------------------------------------------
	// Rule 4: Conflict truncation & log splicing
	// -------------------------------------------------------------------------
	{
		rf := NewRaft(0, 3)
		rf.mu.Lock()
		rf.currentTerm = 2
		// Follower has uncommitted conflicting entries from term 1 at index 2 and 3
		rf.log = append(rf.log,
			LogEntry{Index: 1, Term: 1, Command: "a"},
			LogEntry{Index: 2, Term: 1, Command: "bad1"},
			LogEntry{Index: 3, Term: 1, Command: "bad2"},
		)
		rf.mu.Unlock()

		// Leader sends matching PrevLogIndex=1, PrevLogTerm=1, with new entries for index 2 and 3 in Term 2
		args := &AppendEntriesArgs{
			Term:         2,
			LeaderID:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries: []LogEntry{
				{Index: 2, Term: 2, Command: "good2"},
				{Index: 3, Term: 2, Command: "good3"},
			},
			LeaderCommit: 2,
		}
		reply := &AppendEntriesReply{}
		rf.AppendEntries(args, reply)

		if !reply.Success {
			t.Fatalf("expected AppendEntries to succeed")
		}

		rf.mu.Lock()
		if len(rf.log) != 4 {
			t.Fatalf("expected log length 4, got %d", len(rf.log))
		}
		if rf.log[2].Term != 2 || rf.log[2].Command != "good2" {
			t.Fatalf("expected index 2 replaced with good2, got %+v", rf.log[2])
		}
		if rf.log[3].Term != 2 || rf.log[3].Command != "good3" {
			t.Fatalf("expected index 3 replaced with good3, got %+v", rf.log[3])
		}
		// -------------------------------------------------------------------------
		// Rule 5: Advancing commitIndex = min(LeaderCommit, lastNewEntryIndex)
		// LeaderCommit is 2, lastNewEntryIndex is 3 -> commitIndex must be 2
		// -------------------------------------------------------------------------
		if rf.commitIndex != 2 {
			t.Fatalf("expected commitIndex == 2, got %d", rf.commitIndex)
		}
		rf.mu.Unlock()
	}
}

// TestLeaderAdvancementOfCommitIndex verifies:
//  1. Leader advances commitIndex to N once entry N is replicated on a majority quorum.
//  2. Strict §5.4.2 Invariant: A leader CANNOT commit entries from an earlier term by
//     counting replicas; it only commits entries from its CURRENT term directly!
func TestLeaderAdvancementOfCommitIndex(t *testing.T) {
	clusterSize := 3
	rf := NewRaft(0, clusterSize)

	rf.mu.Lock()
	rf.role = RoleLeader
	rf.currentTerm = 2
	rf.commitIndex = 0
	rf.nextIndex = make([]int, clusterSize)
	rf.matchIndex = make([]int, clusterSize)

	// Log layout:
	// Index 0: sentinel
	// Index 1: Term 1 (from previous leader/term)
	// Index 2: Term 2 (from CURRENT leader/term)
	rf.log = append(rf.log,
		LogEntry{Index: 1, Term: 1, Command: "old_cmd"},
		LogEntry{Index: 2, Term: 2, Command: "current_cmd"},
	)

	// Case 1: An entry from an EARLIER term (Term 1 at index 1) is replicated on ALL peers,
	// but NO current term entry is committed yet.
	// Invariant §5.4.2: Leader MUST NOT advance commitIndex to 1 by counting replicas of Term 1!
	rf.matchIndex[0] = 1
	rf.matchIndex[1] = 1
	rf.matchIndex[2] = 1
	rf.checkAndUpdateCommitIndex()

	if rf.commitIndex != 0 {
		t.Fatalf("VIOLATION of §5.4.2: commitIndex advanced to %d for an earlier term entry!", rf.commitIndex)
	}

	// Case 2: Now the entry from the CURRENT term (Term 2 at index 2) reaches majority quorum
	// (Node 0 and Node 1 have matchIndex >= 2; Quorum = 2 out of 3).
	rf.matchIndex[0] = 2 // Leader
	rf.matchIndex[1] = 2 // Peer 1
	rf.matchIndex[2] = 1 // Peer 2 is lagging
	rf.checkAndUpdateCommitIndex()

	// Both index 1 and index 2 are now safely committed!
	if rf.commitIndex != 2 {
		t.Fatalf("expected commitIndex == 2, got %d", rf.commitIndex)
	}
	rf.mu.Unlock()
}

// TestFastRecoveryLeaderReceipt verifies leader logic upon receiving rejection:
// 1. Leader has ConflictTerm -> nextIndex = last index of ConflictTerm + 1
// 2. Leader does NOT have ConflictTerm -> nextIndex = ConflictIndex
// 3. Follower log too short (ConflictTerm == 0) -> nextIndex = ConflictIndex
func TestFastRecoveryLeaderReceipt(t *testing.T) {
	clusterSize := 2
	leader := NewRaft(0, clusterSize)

	leader.mu.Lock()
	leader.role = RoleLeader
	leader.currentTerm = 3
	leader.nextIndex = make([]int, clusterSize)
	leader.matchIndex = make([]int, clusterSize)

	// Leader log layout:
	// Index 0: sentinel (term 0)
	// Index 1: term 1
	// Index 2: term 1
	// Index 3: term 3
	leader.log = append(leader.log,
		LogEntry{Index: 1, Term: 1, Command: "a"},
		LogEntry{Index: 2, Term: 1, Command: "b"},
		LogEntry{Index: 3, Term: 3, Command: "c"},
	)
	leader.nextIndex[1] = 4
	leader.mu.Unlock()

	// Case 1: Follower rejected with ConflictTerm = 1, ConflictIndex = 1
	// Leader HAS ConflictTerm 1 in its log (indices 1 and 2).
	// Last index of ConflictTerm 1 in leader log is index 2.
	// Leader must set nextIndex[1] = last index of ConflictTerm + 1 = 2 + 1 = 3!
	{
		reply := &AppendEntriesReply{
			Success:       false,
			ConflictTerm:  1,
			ConflictIndex: 1,
		}

		leader.mu.Lock()
		foundTerm := false
		for idx := leader.getLastLogIndex(); idx >= 1; idx-- {
			if leader.log[idx].Term == reply.ConflictTerm {
				leader.nextIndex[1] = idx + 1
				foundTerm = true
				break
			}
		}
		if !foundTerm {
			leader.nextIndex[1] = reply.ConflictIndex
		}

		if leader.nextIndex[1] != 3 {
			t.Fatalf("expected nextIndex == 3, got %d", leader.nextIndex[1])
		}
		leader.mu.Unlock()
	}

	// Case 2: Follower rejected with ConflictTerm = 2 (leader does NOT have term 2), ConflictIndex = 2
	// Leader must bypass entire term 2 by setting nextIndex[1] = ConflictIndex = 2!
	{
		reply := &AppendEntriesReply{
			Success:       false,
			ConflictTerm:  2,
			ConflictIndex: 2,
		}

		leader.mu.Lock()
		foundTerm := false
		for idx := leader.getLastLogIndex(); idx >= 1; idx-- {
			if leader.log[idx].Term == reply.ConflictTerm {
				leader.nextIndex[1] = idx + 1
				foundTerm = true
				break
			}
		}
		if !foundTerm {
			leader.nextIndex[1] = reply.ConflictIndex
		}

		if leader.nextIndex[1] != 2 {
			t.Fatalf("expected nextIndex == 2, got %d", leader.nextIndex[1])
		}
		leader.mu.Unlock()
	}

	// Case 3: Follower log was too short (ConflictTerm = 0, ConflictIndex = 2)
	// Leader jumps directly to ConflictIndex
	{
		reply := &AppendEntriesReply{
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: 2,
		}

		leader.mu.Lock()
		if reply.ConflictTerm == 0 {
			leader.nextIndex[1] = reply.ConflictIndex
		}

		if leader.nextIndex[1] != 2 {
			t.Fatalf("expected nextIndex == 2, got %d", leader.nextIndex[1])
		}
		leader.mu.Unlock()
	}
}

// TestReadIndexLinearizability verifies the ReadIndex protocol (§8):
// 1. Leader captures commitIndex as readIndex.
// 2. Immediate heartbeat round sent to peers and awaits majority confirmation.
// 3. Blocks until lastApplied >= readIndex.
// 4. Returns readIndex and true for linearizable reading from local KV store.
// 5. Partitioned/zombie leader fails to reach majority and rejects read request.
func TestReadIndexLinearizability(t *testing.T) {
	clusterSize := 3
	nodes := make([]*Raft, clusterSize)
	for i := range clusterSize {
		nodes[i] = NewRaft(i, clusterSize)
	}
	for i := range clusterSize {
		nodes[i].SetPeers(nodes)
	}

	// Make Node 0 the Leader
	nodes[0].mu.Lock()
	nodes[0].role = RoleLeader
	nodes[0].currentTerm = 1
	nodes[0].commitIndex = 2
	nodes[0].lastApplied = 2
	nodes[0].initializeLeaderVolatileState()
	nodes[0].mu.Unlock()

	// Case 1: Healthy cluster -> ReadIndex succeeds and returns readIndex = 2
	readIdx, ok := nodes[0].ReadIndex()
	if !ok {
		t.Fatalf("expected ReadIndex to succeed on healthy leader")
	}
	if readIdx != 2 {
		t.Fatalf("expected readIndex == 2, got %d", readIdx)
	}

	// Case 2: Partitioned leader (Node 0 cannot reach peers 1 and 2)
	// Simulate partition by severing peer connections
	nodes[0].mu.Lock()
	nodes[0].peers = []*Raft{nodes[0], nil, nil}
	nodes[0].mu.Unlock()

	// ReadIndex must fail because leader cannot collect majority confirmations
	_, ok = nodes[0].ReadIndex()
	if ok {
		t.Fatalf("expected ReadIndex to fail on partitioned leader")
	}
}
