package raft

import (
	"testing"
	"time"
)

// TestTimerRange verifies that randomized election timeouts stay strictly
// within the [150ms, 300ms] interval.
func TestTimerRange(t *testing.T) {
	rf := NewRaft(0, 3)

	for i := 0; i < 100; i++ {
		timeout := rf.getElectionTimeout()
		if timeout < MinElectionTimeout || timeout >= MaxElectionTimeout {
			t.Fatalf("timeout %v out of bounds [%v, %v)", timeout, MinElectionTimeout, MaxElectionTimeout)
		}
	}
}

// TestFollowerTimesOutToCandidate verifies that an idle follower whose election
// timer expires transitions into a Candidate and increments term.
func TestFollowerTimesOutToCandidate(t *testing.T) {
	rf := NewRaft(0, 3)
	rf.Start()
	defer rf.Stop()

	// Wait up to 400ms (MaxElectionTimeout is 300ms)
	time.Sleep(350 * time.Millisecond)

	if rf.GetRole() != RoleCandidate {
		t.Fatalf("expected node to time out and become Candidate, got %s", rf.GetRole())
	}
	if rf.GetCurrentTerm() < 1 {
		t.Fatalf("expected term >= 1 after becoming candidate, got %d", rf.GetCurrentTerm())
	}
}

// TestFollowerResetsOnHeartbeat verifies that incoming heartbeats prevent
// a follower from timing out and becoming a candidate.
func TestFollowerResetsOnHeartbeat(t *testing.T) {
	rf := NewRaft(0, 3)
	rf.Start()
	defer rf.Stop()

	// Send heartbeats every 40ms for 300ms total.
	// Since 40ms < MinElectionTimeout (150ms), the follower must remain a Follower.
	for i := 0; i < 7; i++ {
		time.Sleep(40 * time.Millisecond)
		sendSignal(rf.heartbeatCh)
	}

	if rf.GetRole() != RoleFollower {
		t.Fatalf("expected node to remain Follower due to heartbeats, got %s", rf.GetRole())
	}
	if rf.GetCurrentTerm() != 0 {
		t.Fatalf("expected term 0, got %d", rf.GetCurrentTerm())
	}
}

