package raft

import (
	"testing"
	"time"
)

// TestNetworkMatrixChaos verifies:
// 1. Initial fully connected mesh.
// 2. Partition(groupA, groupB) cleanly isolates groups.
// 3. Heal() restores complete connectivity.
// 4. DropPercentage(1.0) drops all packets.
// 5. Delay(duration) injects latency.
func TestNetworkMatrixChaos(t *testing.T) {
	clusterSize := 5
	net := NewNetworkMatrix(clusterSize)

	// 1. Fully connected initially
	for i := range clusterSize {
		for j := range clusterSize {
			if !net.CanCommunicate(i, j) {
				t.Fatalf("expected node %d to communicate with %d", i, j)
			}
		}
	}

	// 2. Partition into {0, 1} and {2, 3, 4}
	net.Partition([]int{0, 1}, []int{2, 3, 4})

	// Verify group A internal connectivity
	if !net.CanCommunicate(0, 1) || !net.CanCommunicate(1, 0) {
		t.Fatalf("group A nodes should still communicate internally")
	}

	// Verify group B internal connectivity
	if !net.CanCommunicate(2, 3) || !net.CanCommunicate(3, 4) {
		t.Fatalf("group B nodes should still communicate internally")
	}

	// Verify cross-partition calls are severed
	if net.CanCommunicate(0, 2) || net.CanCommunicate(1, 4) || net.CanCommunicate(3, 0) {
		t.Fatalf("cross-partition communication should be blocked")
	}

	// 3. Heal the partition
	net.Heal()
	if !net.CanCommunicate(0, 2) || !net.CanCommunicate(4, 1) {
		t.Fatalf("network should be healed")
	}

	// 4. Test packet drop
	net.DropPercentage(1.0)
	if net.shouldDeliver(0, 1) {
		t.Fatalf("expected packet to be dropped with 100%% drop rate")
	}
	net.DropPercentage(0.0)
	if !net.shouldDeliver(0, 1) {
		t.Fatalf("expected packet to be delivered with 0%% drop rate")
	}

	// 5. Test synthetic delay
	nodes := make([]*Raft, 2)
	nodes[0] = NewRaft(0, 2)
	nodes[1] = NewRaft(1, 2)
	net2 := NewNetworkMatrix(2)
	net2.RegisterNode(0, nodes[0])
	net2.RegisterNode(1, nodes[1])
	net2.Delay(50 * time.Millisecond)

	start := time.Now()
	args := &AppendEntriesArgs{Term: 1, LeaderID: 0}
	reply := &AppendEntriesReply{}
	net2.CallAppendEntries(0, 1, args, reply)
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected delay >= 40ms, got %v", elapsed)
	}
}
