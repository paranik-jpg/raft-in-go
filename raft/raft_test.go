package raft

import (
	"testing"
)

// TestInitialState verifies that a newly instantiated Raft node conforms
// strictly to the specification for persistent and initial in-memory state.
func TestInitialState(t *testing.T) {
	nodeID := 0
	clusterSize := 3
	rf := NewRaft(nodeID, clusterSize)

	// Invariant: currentTerm is initialized to 0 on first boot
	if rf.currentTerm != 0 {
		t.Fatalf("expected currentTerm == 0, got %d", rf.currentTerm)
	}

	// Invariant: votedFor is initialized to -1 (null/none)
	if rf.votedFor != -1 {
		t.Fatalf("expected votedFor == -1, got %d", rf.votedFor)
	}

	// Invariant: initial role is Follower
	if rf.role != RoleFollower {
		t.Fatalf("expected initial role == RoleFollower, got %s", rf.role)
	}

	// Invariant: volatile commitIndex and lastApplied start at 0
	if rf.commitIndex != 0 {
		t.Fatalf("expected commitIndex == 0, got %d", rf.commitIndex)
	}
	if rf.lastApplied != 0 {
		t.Fatalf("expected lastApplied == 0, got %d", rf.lastApplied)
	}

	// Invariant: nextIndex and matchIndex are uninitialized for non-leaders
	if rf.nextIndex != nil || rf.matchIndex != nil {
		t.Fatalf("expected nextIndex and matchIndex to be nil for followers")
	}

	// Invariant: log must have the sentinel at index 0, length 1
	if len(rf.log) != 1 {
		t.Fatalf("expected log length == 1 (sentinel), got %d", len(rf.log))
	}

	// Invariant: sentinel entry must have Index 0 and Term 0
	if rf.log[0].Index != 0 || rf.log[0].Term != 0 || rf.log[0].Command != nil {
		t.Fatalf("invalid sentinel entry: %+v", rf.log[0])
	}
}

// TestFSMTransitions verifies state transitions:
// - becomeCandidate increments term and votes for self
// - becomeFollower updates term and resets votedFor to -1
// - becomeLeader updates role and initializes nextIndex/matchIndex
func TestFSMTransitions(t *testing.T) {
	clusterSize := 3
	rf := NewRaft(0, clusterSize)

	// Initial: Follower, term 0, votedFor -1
	if rf.GetRole() != RoleFollower || rf.GetCurrentTerm() != 0 {
		t.Fatalf("expected Follower, term 0")
	}

	// 1. Transition to Candidate
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()

	if rf.GetRole() != RoleCandidate {
		t.Fatalf("expected RoleCandidate, got %s", rf.GetRole())
	}
	if rf.GetCurrentTerm() != 1 {
		t.Fatalf("expected term 1, got %d", rf.GetCurrentTerm())
	}
	if rf.votedFor != 0 {
		t.Fatalf("expected votedFor == me (0), got %d", rf.votedFor)
	}

	// 2. Candidate wins election -> Leader
	rf.mu.Lock()
	rf.becomeLeader()
	rf.mu.Unlock()

	if rf.GetRole() != RoleLeader {
		t.Fatalf("expected RoleLeader, got %s", rf.GetRole())
	}
	if len(rf.nextIndex) != clusterSize || len(rf.matchIndex) != clusterSize {
		t.Fatalf("leader volatile state not initialized")
	}

	// 3. Leader discovers higher term (e.g., term 3) -> Follower
	rf.mu.Lock()
	rf.becomeFollower(3)
	rf.mu.Unlock()

	if rf.GetRole() != RoleFollower {
		t.Fatalf("expected RoleFollower, got %s", rf.GetRole())
	}
	if rf.GetCurrentTerm() != 3 {
		t.Fatalf("expected term 3, got %d", rf.GetCurrentTerm())
	}
	if rf.votedFor != -1 {
		t.Fatalf("expected votedFor == -1, got %d", rf.votedFor)
	}
}
