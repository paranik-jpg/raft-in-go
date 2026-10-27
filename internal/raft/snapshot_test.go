package raft

import (
	"reflect"
	"testing"
	"time"
)

// TestLogTruncationSnapshot verifies:
// 1. Snapshot truncates historical log entries up to lastIncludedIndex.
// 2. Preserves lastIncludedIndex and lastIncludedTerm as new sentinel base offset at log[0].
// 3. toSliceIndex correctly converts global indexes to compacted slice offsets.
// 4. Repeated calls with index <= lastIncludedIndex are safely ignored (idempotent).
func TestLogTruncationSnapshot(t *testing.T) {
	rf := NewRaft(0, 3)

	// Simulate 10 committed and applied entries
	rf.mu.Lock()
	rf.currentTerm = 2
	for i := 1; i <= 10; i++ {
		term := 1
		if i > 5 {
			term = 2
		}
		rf.log = append(rf.log, LogEntry{
			Index:   i,
			Term:    term,
			Command: i * 100,
		})
	}
	rf.commitIndex = 10
	rf.lastApplied = 10
	rf.mu.Unlock()

	// Initial assertions before snapshot:
	// log length = 11 (sentinel 0 + 10 entries)
	if len(rf.log) != 11 {
		t.Fatalf("expected initial log length 11, got %d", len(rf.log))
	}
	if rf.getFirstLogIndex() != 0 {
		t.Fatalf("expected initial base offset 0, got %d", rf.getFirstLogIndex())
	}
	if rf.getLastLogIndex() != 10 {
		t.Fatalf("expected last log index 10, got %d", rf.getLastLogIndex())
	}

	// 1. Execute Snapshot at index 6 (lastIncludedIndex = 6, term = 2)
	snapshotData := []byte("dummy_kv_snapshot_at_6")
	rf.Snapshot(6, snapshotData)

	rf.mu.Lock()
	// New log layout:
	// log[0] = sentinel with Index 6, Term 2
	// log[1] = entry 7 (Term 2)
	// log[2] = entry 8 (Term 2)
	// log[3] = entry 9 (Term 2)
	// log[4] = entry 10 (Term 2)
	// Total length must be 5 (compacted from 11 down to 5!)
	if len(rf.log) != 5 {
		t.Fatalf("expected compacted log length 5, got %d", len(rf.log))
	}

	// Invariant: log[0] sentinel must hold lastIncludedIndex 6 and lastIncludedTerm 2
	if rf.log[0].Index != 6 || rf.log[0].Term != 2 {
		t.Fatalf("invalid new sentinel at log[0]: %+v", rf.log[0])
	}

	// Invariant: base offset is now 6
	if rf.getFirstLogIndex() != 6 {
		t.Fatalf("expected base offset 6, got %d", rf.getFirstLogIndex())
	}

	// Invariant: last log index is still 10
	if rf.getLastLogIndex() != 10 {
		t.Fatalf("expected last log index 10, got %d", rf.getLastLogIndex())
	}

	// Invariant: toSliceIndex offset conversion
	// Index 6 -> slice index 0 (sentinel)
	// Index 7 -> slice index 1
	// Index 10 -> slice index 4
	if rf.toSliceIndex(6) != 0 {
		t.Fatalf("expected toSliceIndex(6) == 0, got %d", rf.toSliceIndex(6))
	}
	if rf.toSliceIndex(7) != 1 {
		t.Fatalf("expected toSliceIndex(7) == 1, got %d", rf.toSliceIndex(7))
	}
	if rf.toSliceIndex(10) != 4 {
		t.Fatalf("expected toSliceIndex(10) == 4, got %d", rf.toSliceIndex(10))
	}

	// Check entries 7..10 preserved correctly
	for idx := 7; idx <= 10; idx++ {
		sliceIdx := rf.toSliceIndex(idx)
		if rf.log[sliceIdx].Index != idx || rf.log[sliceIdx].Command != idx*100 {
			t.Fatalf("entry corrupted at index %d: %+v", idx, rf.log[sliceIdx])
		}
	}
	rf.mu.Unlock()

	// 2. Snapshot idempotency: snapshotting at index <= baseIndex (e.g. index 4 or 6) must be ignored
	rf.Snapshot(4, []byte("stale"))
	rf.Snapshot(6, []byte("stale"))

	rf.mu.Lock()
	if len(rf.log) != 5 || rf.getFirstLogIndex() != 6 {
		t.Fatalf("stale snapshot modified compacted log")
	}
	rf.mu.Unlock()

	// 3. Verify snapshot persisted to storage
	savedSnap, lastIdx, lastTerm, err := rf.storage.ReadSnapshot()
	if err != nil {
		t.Fatalf("failed to read snapshot: %v", err)
	}
	if lastIdx != 6 || lastTerm != 2 {
		t.Fatalf("expected storage metadata (6, 2), got (%d, %d)", lastIdx, lastTerm)
	}
	if !reflect.DeepEqual(savedSnap, snapshotData) {
		t.Fatalf("expected snapshot payload %v, got %v", snapshotData, savedSnap)
	}
}

// TestInstallSnapshotFollowerHandler verifies the follower's receiver logic for InstallSnapshot (§7):
// 1. Stale term is rejected.
// 2. Term promotion and stepdown to follower.
// 3. Stale snapshot is safely ignored.
// 4. Follower state machine receives ApplyMsg with SnapshotValid = true across applyCh.
// 5. Follower log discarded and reset to new sentinel log[0].
func TestInstallSnapshotFollowerHandler(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	rf := NewRaft(0, 3, applyCh)

	rf.mu.Lock()
	rf.currentTerm = 2
	rf.role = RoleCandidate
	rf.mu.Unlock()

	// 1. Stale term rejected
	reply := &InstallSnapshotReply{}
	rf.InstallSnapshot(&InstallSnapshotArgs{
		Term:              1,
		LeaderID:          1,
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              []byte("snap1"),
	}, reply)

	if reply.Term != 2 {
		t.Fatalf("expected reply term 2, got %d", reply.Term)
	}

	// 2. Valid snapshot from leader with Term 3
	snapPayload := []byte("serialized_kv_snapshot_data")
	reply2 := &InstallSnapshotReply{}
	rf.InstallSnapshot(&InstallSnapshotArgs{
		Term:              3,
		LeaderID:          1,
		LastIncludedIndex: 15,
		LastIncludedTerm:  3,
		Data:              snapPayload,
	}, reply2)

	// Follower must have adopted Term 3 and stepped down to Follower
	rf.mu.Lock()
	if rf.currentTerm != 3 || rf.role != RoleFollower {
		t.Fatalf("follower failed to update to term 3 follower, got term %d role %v", rf.currentTerm, rf.role)
	}
	if rf.getFirstLogIndex() != 15 || rf.getLastLogIndex() != 15 {
		t.Fatalf("log base failed to reset to 15, got first %d last %d", rf.getFirstLogIndex(), rf.getLastLogIndex())
	}
	if rf.log[0].Term != 3 {
		t.Fatalf("expected sentinel term 3, got %d", rf.log[0].Term)
	}
	if rf.commitIndex != 15 || rf.lastApplied != 15 {
		t.Fatalf("expected commitIndex and lastApplied 15, got (%d, %d)", rf.commitIndex, rf.lastApplied)
	}
	rf.mu.Unlock()

	// 3. State machine must have received Snapshot message over applyCh
	select {
	case msg := <-applyCh:
		if !msg.SnapshotValid {
			t.Fatalf("expected SnapshotValid = true")
		}
		if msg.SnapshotIndex != 15 || msg.SnapshotTerm != 3 {
			t.Fatalf("expected snapshot index 15 term 3, got (%d, %d)", msg.SnapshotIndex, msg.SnapshotTerm)
		}
		if !reflect.DeepEqual(msg.Snapshot, snapPayload) {
			t.Fatalf("snapshot payload mismatch")
		}
	default:
		t.Fatalf("failed to receive snapshot on applyCh")
	}
}

// TestLeaderSendsInstallSnapshotToLaggingFollower verifies end-to-end integration:
// 1. Leader commits 10 entries and takes a snapshot at index 8.
// 2. Leader attempts to replicate to a lagging follower whose nextIndex is 1.
// 3. Leader detects nextIndex <= log[0].Index and transmits InstallSnapshot RPC.
// 4. Follower catches up, receives snapshot on applyCh, and leader updates matchIndex.
func TestLeaderSendsInstallSnapshotToLaggingFollower(t *testing.T) {
	applyChFollower := make(chan ApplyMsg, 20)
	leader := NewRaft(0, 2)
	follower := NewRaft(1, 2, applyChFollower)

	leader.peers = []*Raft{leader, follower}
	follower.peers = []*Raft{leader, follower}

	// Make node 0 the leader in Term 1
	leader.mu.Lock()
	leader.becomeLeader()
	leader.currentTerm = 1
	leader.nextIndex[1] = 1
	leader.matchIndex[1] = 0

	for i := 1; i <= 10; i++ {
		leader.log = append(leader.log, LogEntry{
			Index:   i,
			Term:    1,
			Command: i * 10,
		})
	}
	leader.commitIndex = 10
	leader.lastApplied = 10
	leader.mu.Unlock()

	// Leader snapshots at index 8
	snapBytes := []byte("cluster_snapshot_at_8")
	leader.Snapshot(8, snapBytes)

	// Leader now triggers replication (heartbeat round)
	leader.sendHeartbeats()

	// Follower should receive InstallSnapshot and deliver ApplyMsg
	select {
	case msg := <-applyChFollower:
		if !msg.SnapshotValid {
			t.Fatalf("expected SnapshotValid = true")
		}
		if msg.SnapshotIndex != 8 || msg.SnapshotTerm != 1 {
			t.Fatalf("expected snapshot index 8 term 1, got (%d, %d)", msg.SnapshotIndex, msg.SnapshotTerm)
		}
		if !reflect.DeepEqual(msg.Snapshot, snapBytes) {
			t.Fatalf("payload mismatch")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for follower to receive snapshot")
	}

	// Verify leader advanced matchIndex and nextIndex for follower
	leader.mu.Lock()
	if leader.matchIndex[1] != 8 {
		t.Fatalf("expected leader matchIndex[1] == 8, got %d", leader.matchIndex[1])
	}
	if leader.nextIndex[1] != 9 {
		t.Fatalf("expected leader nextIndex[1] == 9, got %d", leader.nextIndex[1])
	}
	leader.mu.Unlock()

	// Verify follower log state is compacted up to 8
	follower.mu.Lock()
	if follower.getFirstLogIndex() != 8 {
		t.Fatalf("expected follower base 8, got %d", follower.getFirstLogIndex())
	}
	follower.mu.Unlock()
}
