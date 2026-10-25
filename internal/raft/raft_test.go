package raft

import (
	"testing"
	"time"
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

// TestApplyLoopDecoupledExecution verifies:
// 1. applyLoop monitors commitIndex > lastApplied.
// 2. Automatically increments lastApplied.
// 3. Emits ApplyMsg sequentially across applyCh without holding the mutex.
func TestApplyLoopDecoupledExecution(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	rf := NewRaft(0, 3, applyCh)

	// Add 3 log entries: index 1, 2, 3
	rf.mu.Lock()
	rf.log = append(rf.log,
		LogEntry{Index: 1, Term: 1, Command: "cmd1"},
		LogEntry{Index: 2, Term: 1, Command: "cmd2"},
		LogEntry{Index: 3, Term: 1, Command: "cmd3"},
	)
	rf.mu.Unlock()

	// Start Raft node (spawns run and applyLoop)
	rf.Start()
	defer rf.Stop()

	// Advance commitIndex to 2 under lock and broadcast condition
	rf.mu.Lock()
	rf.commitIndex = 2
	rf.applyCond.Broadcast()
	rf.mu.Unlock()

	// Verify that applyCh receives cmd1 and cmd2 in strict order
	expected := []struct {
		index   int
		command any
	}{
		{index: 1, command: "cmd1"},
		{index: 2, command: "cmd2"},
	}

	for _, exp := range expected {
		select {
		case msg := <-applyCh:
			if !msg.CommandValid {
				t.Fatalf("expected CommandValid == true")
			}
			if msg.CommandIndex != exp.index {
				t.Fatalf("expected CommandIndex %d, got %d", exp.index, msg.CommandIndex)
			}
			if msg.Command != exp.command {
				t.Fatalf("expected Command %v, got %v", exp.command, msg.Command)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for applyMsg for index %d", exp.index)
		}
	}

	// Verify lastApplied has advanced to 2
	time.Sleep(50 * time.Millisecond)
	rf.mu.Lock()
	if rf.lastApplied != 2 {
		t.Fatalf("expected lastApplied == 2, got %d", rf.lastApplied)
	}
	rf.mu.Unlock()

	// Now advance commitIndex to 3
	rf.mu.Lock()
	rf.commitIndex = 3
	rf.applyCond.Broadcast()
	rf.mu.Unlock()

	// Expect cmd3
	select {
	case msg := <-applyCh:
		if msg.CommandIndex != 3 || msg.Command != "cmd3" {
			t.Fatalf("unexpected msg: %+v", msg)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for applyMsg for index 3")
	}

	time.Sleep(50 * time.Millisecond)
	rf.mu.Lock()
	if rf.lastApplied != 3 {
		t.Fatalf("expected lastApplied == 3, got %d", rf.lastApplied)
	}
	rf.mu.Unlock()
}
