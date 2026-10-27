package raft

import (
	"math/rand"
	"sync"
	"time"
)

// NetworkMatrix is a simulated programmable network switch.
// It intercepts all RPC calls between nodes to simulate real-world distributed chaos:
// dynamic partitions, packet loss, simulated network delays, and cluster healing.
type NetworkMatrix struct {
	mu         sync.RWMutex
	nodesCount int

	// connected[from][to] tracks whether 'from' can send packets to 'to'.
	// Supports symmetric and asymmetric partitions.
	connected [][]bool

	// dropRate is a float in [0.0, 1.0] representing packet loss probability.
	dropRate float64

	// delay is the synthetic latency injected before delivering an RPC.
	delay time.Duration

	// nodes holds references to the consensus nodes for in-memory RPC dispatching.
	nodes []*Raft
}

// NewNetworkMatrix initializes a fully-connected N x N network switch.
func NewNetworkMatrix(nodesCount int) *NetworkMatrix {
	matrix := make([][]bool, nodesCount)
	for i := range nodesCount {
		matrix[i] = make([]bool, nodesCount)
		for j := range nodesCount {
			matrix[i][j] = true // Fully connected initially
		}
	}

	return &NetworkMatrix{
		nodesCount: nodesCount,
		connected:  matrix,
		dropRate:   0.0,
		delay:      0,
		nodes:      make([]*Raft, nodesCount),
	}
}

// RegisterNode attaches a Raft node instance to the network switch.
func (nm *NetworkMatrix) RegisterNode(id int, rf *Raft) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if id >= 0 && id < nm.nodesCount {
		nm.nodes[id] = rf
	}
}

// CanCommunicate checks if Node A can reach Node B.
func (nm *NetworkMatrix) CanCommunicate(from, to int) bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	if from < 0 || from >= nm.nodesCount || to < 0 || to >= nm.nodesCount {
		return false
	}
	return nm.connected[from][to]
}

// Partition dynamically isolates groupA from groupB.
// Nodes within groupA can still communicate with each other.
// Nodes within groupB can still communicate with each other.
// Packets across the groupA <-> groupB boundary are severed.
func (nm *NetworkMatrix) Partition(groupA, groupB []int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	setA := make(map[int]bool)
	for _, id := range groupA {
		setA[id] = true
	}

	setB := make(map[int]bool)
	for _, id := range groupB {
		setB[id] = true
	}

	for a := range setA {
		for b := range setB {
			nm.connected[a][b] = false
			nm.connected[b][a] = false
		}
	}
}

// Heal restores full mesh connectivity across all nodes, clearing partitions.
func (nm *NetworkMatrix) Heal() {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	for i := 0; i < nm.nodesCount; i++ {
		for j := 0; j < nm.nodesCount; j++ {
			nm.connected[i][j] = true
		}
	}
}

// Delay injects a synthetic duration of latency into every RPC dispatch.
func (nm *NetworkMatrix) Delay(duration time.Duration) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.delay = duration
}

// DropPercentage configures a probabilistic packet drop rate in [0.0, 1.0].
func (nm *NetworkMatrix) DropPercentage(rate float64) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if rate < 0.0 {
		rate = 0.0
	} else if rate > 1.0 {
		rate = 1.0
	}
	nm.dropRate = rate
}

// shouldDeliver determines whether an RPC packet is allowed to pass through
// the simulated network switch.
func (nm *NetworkMatrix) shouldDeliver(from, to int) bool {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	// 1. Connection matrix check: are the nodes partitioned?
	if !nm.connected[from][to] {
		return false
	}

	// 2. Probabilistic packet drop check
	if nm.dropRate > 0.0 && rand.Float64() < nm.dropRate {
		return false
	}

	return true
}

// CallRequestVote intercepts and delivers a RequestVote RPC through the network switch.
func (nm *NetworkMatrix) CallRequestVote(from, to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if !nm.shouldDeliver(from, to) {
		return false
	}

	nm.mu.RLock()
	delay := nm.delay
	target := nm.nodes[to]
	nm.mu.RUnlock()

	if target == nil {
		return false
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	target.RequestVote(args, reply)

	// Also check if reply packet is allowed to return back (symmetric channel check)
	return nm.shouldDeliver(to, from)
}

// CallAppendEntries intercepts and delivers an AppendEntries RPC through the network switch.
func (nm *NetworkMatrix) CallAppendEntries(from, to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if !nm.shouldDeliver(from, to) {
		return false
	}

	nm.mu.RLock()
	delay := nm.delay
	target := nm.nodes[to]
	nm.mu.RUnlock()

	if target == nil {
		return false
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	target.AppendEntries(args, reply)

	// Also check if reply packet is allowed to return back
	return nm.shouldDeliver(to, from)
}

// CallInstallSnapshot intercepts and delivers an InstallSnapshot RPC through the network switch.
func (nm *NetworkMatrix) CallInstallSnapshot(from, to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if !nm.shouldDeliver(from, to) {
		return false
	}

	nm.mu.RLock()
	delay := nm.delay
	target := nm.nodes[to]
	nm.mu.RUnlock()

	if target == nil {
		return false
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	target.InstallSnapshot(args, reply)

	// Also check if reply packet is allowed to return back
	return nm.shouldDeliver(to, from)
}

