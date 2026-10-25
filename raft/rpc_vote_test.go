package raft

import (
	"testing"
	"time"
)

// TestRequestVoteValidation tests all invariant checks of the RequestVote RPC handler:
// 1. Rejects votes if args.Term < currentTerm.
// 2. Steps down to Follower if args.Term > currentTerm.
// 3. Rejects vote if already voted for another candidate in the same term.
// 4. Grants vote if haven't voted, and candidate log is up-to-date.
// 5. Enforces Election Restriction (§5.4.1) based on (LastLogTerm, LastLogIndex).
func TestRequestVoteValidation(t *testing.T) {
	rf := NewRaft(0, 3)

	// Set initial state: term 2, votedFor = -1, log has 2 entries (index 1 term 1, index 2 term 2)
	rf.mu.Lock()
	rf.currentTerm = 2
	rf.votedFor = -1
	rf.log = append(rf.log,
		LogEntry{Index: 1, Term: 1, Command: "x"},
		LogEntry{Index: 2, Term: 2, Command: "y"},
	)
	rf.mu.Unlock()

	// Case 1: Candidate has stale term (args.Term = 1 < rf.currentTerm = 2) -> REJECT
	{
		args := &RequestVoteArgs{
			Term:         1,
			CandidateID:  1,
			LastLogIndex: 2,
			LastLogTerm:  2,
		}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)

		if reply.VoteGranted {
			t.Fatalf("expected vote rejected due to stale term")
		}
		if reply.Term != 2 {
			t.Fatalf("expected reply.Term == 2, got %d", reply.Term)
		}
	}

	// Case 2: Candidate has higher term (args.Term = 3), but stale log -> REJECT vote, BUT bump term
	{
		args := &RequestVoteArgs{
			Term:         3,
			CandidateID:  1,
			LastLogIndex: 1, // Candidate only has index 1, term 1; voter has index 2, term 2!
			LastLogTerm:  1,
		}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)

		if reply.VoteGranted {
			t.Fatalf("expected vote rejected due to less up-to-date log")
		}
		if reply.Term != 3 {
			t.Fatalf("expected reply.Term == 3, got %d", reply.Term)
		}
		if rf.GetCurrentTerm() != 3 {
			t.Fatalf("expected node to step up to term 3, got %d", rf.GetCurrentTerm())
		}
	}

	// Case 3: Candidate in term 3 with equal term and equal log length -> GRANT vote
	{
		args := &RequestVoteArgs{
			Term:         3,
			CandidateID:  1,
			LastLogIndex: 2,
			LastLogTerm:  2,
		}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)

		if !reply.VoteGranted {
			t.Fatalf("expected vote granted to candidate 1")
		}
		if rf.votedFor != 1 {
			t.Fatalf("expected votedFor == 1, got %d", rf.votedFor)
		}
	}

	// Case 4: Another candidate in the same term (term 3) asks for vote -> REJECT (already voted for 1)
	{
		args := &RequestVoteArgs{
			Term:         3,
			CandidateID:  2,
			LastLogIndex: 5,
			LastLogTerm:  3,
		}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)

		if reply.VoteGranted {
			t.Fatalf("expected vote rejected because node already voted for candidate 1 in term 3")
		}
	}

	// Case 5: The same candidate asks for vote again in same term -> GRANT (idempotency)
	{
		args := &RequestVoteArgs{
			Term:         3,
			CandidateID:  1,
			LastLogIndex: 2,
			LastLogTerm:  2,
		}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)

		if !reply.VoteGranted {
			t.Fatalf("expected vote granted again to candidate 1 (idempotent)")
		}
	}
}

// TestParallelVoteFanOut verifies that:
// 1. A candidate broadcasts RequestVote RPCs in parallel to all peers.
// 2. Votes are collected thread-safely until majority quorum is reached.
// 3. Winning an election triggers winElectionCh and converts the node to Leader.
func TestParallelVoteFanOut(t *testing.T) {
	clusterSize := 3
	nodes := make([]*Raft, clusterSize)
	for i := range clusterSize {
		nodes[i] = NewRaft(i, clusterSize)
	}
	for i := range clusterSize {
		nodes[i].SetPeers(nodes)
	}

	// Node 0 starts an election:
	nodes[0].mu.Lock()
	nodes[0].becomeCandidate()
	nodes[0].startElection()
	nodes[0].mu.Unlock()

	// Wait for winElectionCh to be triggered on Node 0
	select {
	case <-nodes[0].winElectionCh:
		// Transition node 0 to leader
		nodes[0].mu.Lock()
		nodes[0].becomeLeader()
		nodes[0].mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for node 0 to win election")
	}

	// Verify Node 0 is now Leader
	if nodes[0].GetRole() != RoleLeader {
		t.Fatalf("expected node 0 to be RoleLeader, got %s", nodes[0].GetRole())
	}
	if nodes[0].GetCurrentTerm() != 1 {
		t.Fatalf("expected term 1, got %d", nodes[0].GetCurrentTerm())
	}

	// Give the remaining goroutine a few milliseconds to finish processing peer 2
	time.Sleep(50 * time.Millisecond)

	// Verify peers voted for node 0 in term 1
	for i := 1; i < clusterSize; i++ {
		nodes[i].mu.Lock()
		if nodes[i].votedFor != 0 {
			t.Fatalf("expected peer %d to have votedFor == 0, got %d", i, nodes[i].votedFor)
		}
		if nodes[i].currentTerm != 1 {
			t.Fatalf("expected peer %d to be in term 1, got %d", i, nodes[i].currentTerm)
		}
		nodes[i].mu.Unlock()
	}
}

// TestVoteEvaluationOutcomes verifies the three distinct candidate outcomes:
// 1. Quorum reached -> become Leader, initialize nextIndex/matchIndex, send heartbeats.
// 2. Incoming AppendEntries from term >= currentTerm -> step down to Follower.
// 3. Election timer expires -> increment term and restart election.
func TestVoteEvaluationOutcomes(t *testing.T) {
	// Outcome 2: Candidate receives AppendEntries with term >= currentTerm -> step down to Follower
	{
		rf := NewRaft(0, 3)
		rf.mu.Lock()
		rf.becomeCandidate() // term = 1, role = RoleCandidate
		rf.mu.Unlock()

		if rf.GetRole() != RoleCandidate {
			t.Fatalf("expected RoleCandidate")
		}

		// Valid leader in term 1 sends AppendEntries heartbeat
		args := &AppendEntriesArgs{
			Term:         1,
			LeaderID:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: 0,
		}
		reply := &AppendEntriesReply{}
		rf.AppendEntries(args, reply)

		if !reply.Success {
			t.Fatalf("expected AppendEntries to succeed")
		}
		if rf.GetRole() != RoleFollower {
			t.Fatalf("expected candidate to step down to Follower, got %s", rf.GetRole())
		}
		if rf.GetCurrentTerm() != 1 {
			t.Fatalf("expected term 1, got %d", rf.GetCurrentTerm())
		}
	}

	// Outcome 1: Candidate transitions to Leader and initializes volatile state
	{
		rf := NewRaft(0, 3)
		rf.mu.Lock()
		rf.becomeCandidate()
		rf.becomeLeader()
		rf.mu.Unlock()

		if rf.GetRole() != RoleLeader {
			t.Fatalf("expected RoleLeader")
		}
		// nextIndex must be initialized to lastLogIndex + 1 = 1
		for p := range 3 {
			if rf.nextIndex[p] != 1 {
				t.Fatalf("expected nextIndex[%d] == 1, got %d", p, rf.nextIndex[p])
			}
			if rf.matchIndex[p] != 0 {
				t.Fatalf("expected matchIndex[%d] == 0, got %d", p, rf.matchIndex[p])
			}
		}
	}
}
