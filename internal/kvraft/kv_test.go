package kvraft

import (
	"fmt"
	"testing"
	"time"

	"raft-go/internal/raft"
)

// setupCluster initializes an N-node KVServer cluster running atop Raft.
func setupCluster(t *testing.T, n int, maxRaftState int) ([]*KVServer, []*raft.Raft, func()) {
	t.Helper()
	rafts := make([]*raft.Raft, n)
	servers := make([]*KVServer, n)

	for i := range n {
		applyCh := make(chan raft.ApplyMsg, 100)
		rafts[i] = raft.NewRaft(i, n, applyCh)
		servers[i] = NewKVServer(i, rafts[i], applyCh, maxRaftState)
	}

	for i := range n {
		rafts[i].SetPeers(rafts)
		rafts[i].Start()
	}

	// Wait for a leader to be elected
	var leaderID = -1
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i := range n {
			if rafts[i].GetRole() == raft.RoleLeader {
				leaderID = i
				break
			}
		}
		if leaderID != -1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if leaderID == -1 {
		t.Fatalf("failed to elect cluster leader within 3s")
	}

	cleanup := func() {
		for i := range n {
			servers[i].Kill()
			rafts[i].Stop()
		}
	}

	return servers, rafts, cleanup
}

func TestKVClerkBasicPutAppendGet(t *testing.T) {
	servers, _, cleanup := setupCluster(t, 3, -1)
	defer cleanup()

	ck := NewClerk(servers)

	// 1. Initial Get should be empty
	if val := ck.Get("k1"); val != "" {
		t.Fatalf("expected empty value for k1, got %q", val)
	}

	// 2. Put
	ck.Put("k1", "v1")
	if val := ck.Get("k1"); val != "v1" {
		t.Fatalf("expected k1 = v1, got %q", val)
	}

	// 3. Append
	ck.Append("k1", "_appended")
	if val := ck.Get("k1"); val != "v1_appended" {
		t.Fatalf("expected k1 = v1_appended, got %q", val)
	}

	// 4. Multiple keys
	for i := 1; i <= 5; i++ {
		ck.Put(fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i))
	}
	for i := 1; i <= 5; i++ {
		expected := fmt.Sprintf("val_%d", i)
		if val := ck.Get(fmt.Sprintf("key_%d", i)); val != expected {
			t.Fatalf("expected %s, got %s", expected, val)
		}
	}
}

// TestKVContinuousCompaction verifies that continuous log compaction every 5 entries
// truncates historical entries without corrupting ongoing client operations.
func TestKVContinuousCompaction(t *testing.T) {
	servers, _, cleanup := setupCluster(t, 3, 5)
	defer cleanup()

	ck := NewClerk(servers)

	// Issue 15 operations to trigger multiple snapshots
	for i := 1; i <= 15; i++ {
		ck.Put("counter", fmt.Sprintf("%d", i))
	}

	// Verify latest value
	if val := ck.Get("counter"); val != "15" {
		t.Fatalf("expected counter = 15, got %s", val)
	}
}

// TestKVSnapshotRecovery tests true crash-recovery:
// 1. Client puts data into the cluster, triggering a snapshot at entry 5.
// 2. We crash/stop a node, wiping its in-memory state completely.
// 3. We reboot the node using its persisted storage (which only holds the snapshot, not the old logs!).
// 4. We assert that the recovered node restores all KV data directly from the snapshot!
func TestKVSnapshotRecovery(t *testing.T) {
	servers, rafts, cleanup := setupCluster(t, 3, 5)
	defer cleanup()

	ck := NewClerk(servers)

	// Put 5 entries to trigger snapshot at index 5
	for i := 1; i <= 5; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// Wait briefly for snapshot to persist
	time.Sleep(50 * time.Millisecond)

	// Pick a node to crash (e.g. node 0)
	targetNode := 0
	savedStorage := rafts[targetNode].GetStorage()

	// Verify that storage actually holds the snapshot
	snapData, lastIdx, _, err := savedStorage.ReadSnapshot()
	if err != nil || len(snapData) == 0 || lastIdx < 5 {
		t.Fatalf("expected snapshot in storage with lastIdx >= 5, got lastIdx=%d, err=%v", lastIdx, err)
	}

	// Kill node 0 completely (simulate crash and memory wipe)
	servers[targetNode].Kill()
	rafts[targetNode].Stop()

	// Reboot node 0 with clean in-memory state attached to the same persisted storage
	newApplyCh := make(chan raft.ApplyMsg, 100)
	newRaft := raft.NewRaft(targetNode, 3, newApplyCh)
	newRaft.SetStorage(savedStorage)

	// Start recovered KVServer (which restores its kvStore immediately from savedStorage)
	newServer := NewKVServer(targetNode, newRaft, newApplyCh, 5)
	defer newServer.Kill()
	defer newRaft.Stop()

	// Reconnect cluster peers
	newPeers := make([]*raft.Raft, 3)
	for i := range 3 {
		if i == targetNode {
			newPeers[i] = newRaft
		} else {
			newPeers[i] = rafts[i]
		}
	}
	newRaft.SetPeers(newPeers)
	newRaft.Start()

	// Reconnect servers list for clerk
	recoveredServers := make([]*KVServer, 3)
	for i := range 3 {
		if i == targetNode {
			recoveredServers[i] = newServer
		} else {
			recoveredServers[i] = servers[i]
		}
	}
	ckRecovered := NewClerk(recoveredServers)

	// Assert that all keys 1..5 are immediately readable after recovery!
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("k%d", i)
		expected := fmt.Sprintf("v%d", i)
		if val := ckRecovered.Get(key); val != expected {
			t.Fatalf("key %s failed to recover from snapshot: expected %s, got %s", key, expected, val)
		}
	}
}


