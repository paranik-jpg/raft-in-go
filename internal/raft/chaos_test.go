package raft

import (
	"reflect"
	"testing"
	"time"
)

// TestChaosA_MajorityPartition asserts:
// 1. Form a 5-node cluster.
// 2. Partition into {N0, N1} (minority) and {N2, N3, N4} (majority).
// 3. Send writes to both sides.
// 4. Verify {N0, N1} stops committing.
// 5. Verify {N2, N3, N4} elects a leader and continues committing.
//
// TestChaosB_PartitionHealingAndReconciliation asserts:
// 1. Heal the partition.
// 2. Send a new write to the authoritative leader.
// 3. Verify N0 and N1 step down to follower.
// 4. Verify N0 and N1 truncate their uncommitted diverged logs.
// 5. Verify all 5 nodes adopt the leader's log and achieve identical state machine snapshots!
func TestChaosPartitionAndHealing(t *testing.T) {
	clusterSize := 5
	nodes := make([]*Raft, clusterSize)
	applyChs := make([]chan ApplyMsg, clusterSize)
	net := NewNetworkMatrix(clusterSize)

	for i := range clusterSize {
		applyChs[i] = make(chan ApplyMsg, 100)
		nodes[i] = NewRaft(i, clusterSize, applyChs[i])
		nodes[i].SetNetworkMatrix(net)
		net.RegisterNode(i, nodes[i])
	}
	for i := range clusterSize {
		nodes[i].SetPeers(nodes)
	}

	// Start all nodes
	for i := range clusterSize {
		nodes[i].Start()
		defer nodes[i].Stop()
	}

	// -------------------------------------------------------------------------
	// Initial Peacetime: Wait for an initial leader to emerge
	// -------------------------------------------------------------------------
	var initialLeader *Raft
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i := range clusterSize {
			if nodes[i].GetRole() == RoleLeader {
				initialLeader = nodes[i]
				break
			}
		}
		if initialLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if initialLeader == nil {
		t.Fatalf("failed to elect initial leader")
	}

	// Submit an initial command to verify cluster health
	idx, _, isLeader := initialLeader.StartCommand("initial_cmd")
	if !isLeader {
		t.Fatalf("initial leader rejected command")
	}

	// Verify command committed across quorum
	time.Sleep(200 * time.Millisecond)
	if initialLeader.GetCommitIndex() < idx {
		t.Fatalf("expected initial_cmd to be committed, got commitIndex %d", initialLeader.GetCommitIndex())
	}

	// =========================================================================
	// TEST A: MAJORITY PARTITION
	// Partition cluster into Minority {N0, N1} and Majority {N2, N3, N4}
	// Ensure initial leader is in Minority group for maximum chaos!
	// =========================================================================
	minorityGroup := []int{0, 1}
	majorityGroup := []int{2, 3, 4}

	// If initialLeader was in majority, swap IDs so leader is in minority
	if initialLeader.me >= 2 {
		// Put initial leader in minority
		minorityGroup = []int{initialLeader.me, (initialLeader.me + 1) % 5}
		var maj []int
		for i := range 5 {
			if i != minorityGroup[0] && i != minorityGroup[1] {
				maj = append(maj, i)
			}
		}
		majorityGroup = maj
	}

	// Sever network between minority and majority
	net.Partition(minorityGroup, majorityGroup)

	minorityLeaderID := minorityGroup[0]
	minorityLeader := nodes[minorityLeaderID]

	// Send write to the minority side
	minorityCommitBefore := minorityLeader.GetCommitIndex()
	minIdx, _, isMinLeader := minorityLeader.StartCommand("uncommitted_minority_cmd")

	// Wait 300ms (longer than election timeout and heartbeats)
	time.Sleep(300 * time.Millisecond)

	// Invariant A1: Minority MUST NOT commit the write!
	if isMinLeader && minorityLeader.GetCommitIndex() > minorityCommitBefore {
		t.Fatalf("VIOLATION: Minority partitioned leader committed entry %d without quorum!", minIdx)
	}

	// Invariant A2: Majority partition {N3, N4, N5} MUST elect a new leader and commit!
	var majorityLeader *Raft
	majDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(majDeadline) {
		for _, id := range majorityGroup {
			if nodes[id].GetRole() == RoleLeader {
				majorityLeader = nodes[id]
				break
			}
		}
		if majorityLeader != nil {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if majorityLeader == nil {
		t.Fatalf("majority partition failed to elect a new leader")
	}

	// Submit write to the majority leader
	majIdx, _, isMajLeader := majorityLeader.StartCommand("majority_committed_cmd")
	if !isMajLeader {
		t.Fatalf("majority leader failed to accept command")
	}

	// Wait for replication across majority
	time.Sleep(250 * time.Millisecond)

	if majorityLeader.GetCommitIndex() < majIdx {
		t.Fatalf("expected majority leader to commit %d, got %d", majIdx, majorityLeader.GetCommitIndex())
	}

	// =========================================================================
	// TEST B: PARTITION HEALING & RECONCILIATION
	// 1. Heal the partition.
	// 2. Send a new write to the authoritative leader.
	// 3. Verify N1 and N2 step down.
	// 4. Verify N1 and N2 truncate their diverged uncommitted entries.
	// 5. Verify all 5 nodes adopt the leader's log with identical snapshots!
	// =========================================================================
	net.Heal()

	// Give the network 150ms to exchange heartbeats and terms
	time.Sleep(150 * time.Millisecond)

	// Invariant B1: Old minority leader MUST step down to Follower
	if minorityLeader.GetRole() == RoleLeader {
		t.Fatalf("expected old minority leader to step down to Follower upon heal")
	}

	// Send a final write to the cluster through the true leader
	finalIdx, _, isFinalLeader := majorityLeader.StartCommand("healed_reconciled_cmd")
	if !isFinalLeader {
		t.Fatalf("expected majority leader to accept final command")
	}

	// Allow replication and log truncation to settle across all 5 nodes
	time.Sleep(400 * time.Millisecond)

	if majorityLeader.GetCommitIndex() < finalIdx {
		t.Fatalf("expected final command to commit, got commitIndex %d", majorityLeader.GetCommitIndex())
	}

	// Invariant B2 & B3: Truncation & State Machine Safety across ALL 5 nodes
	// All 5 nodes must have the exact same committed commands up to finalIdx
	majorityLeader.mu.Lock()
	authoritativeLog := make([]LogEntry, len(majorityLeader.log))
	copy(authoritativeLog, majorityLeader.log)
	majorityLeader.mu.Unlock()

	for i := range clusterSize {
		nodes[i].mu.Lock()
		nodeLog := nodes[i].log
		commit := nodes[i].commitIndex
		nodes[i].mu.Unlock()

		if commit < finalIdx {
			t.Fatalf("expected node %d to commit up to %d, got %d", i, finalIdx, commit)
		}

		// Verify every entry up to commitIndex matches the authoritative log
		for idx := 1; idx <= finalIdx; idx++ {
			if nodeLog[idx].Term != authoritativeLog[idx].Term ||
				!reflect.DeepEqual(nodeLog[idx].Command, authoritativeLog[idx].Command) {
				t.Fatalf("LOG DIVERGENCE on node %d at index %d: node has %+v, leader has %+v",
					i, idx, nodeLog[idx], authoritativeLog[idx])
			}
		}
	}
}
