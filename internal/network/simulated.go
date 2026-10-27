package network

import (
	"math/rand"
	"sync"
	"time"
)

// RPCEndpoint represents a callable Raft node endpoint capable of handling
// RequestVote, AppendEntries, and InstallSnapshot RPC calls.
type RPCEndpoint interface {
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply)
	AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)
	InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)
}

// NetworkSwitch is the simulated network switch interface supporting chaos injection.
type NetworkSwitch interface {
	CanCommunicate(from, to int) bool
	Partition(groupA, groupB []int)
	Heal()
	Delay(d time.Duration)
	DropPercentage(rate float64)
	CallRequestVote(from, to int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	CallAppendEntries(from, to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
	CallInstallSnapshot(from, to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool
}

// SimulatedNetwork implements NetworkSwitch in-memory for testing and chaos simulation.
type SimulatedNetwork struct {
	mu         sync.RWMutex
	nodesCount int
	connected  [][]bool
	dropRate   float64
	delay      time.Duration
	endpoints  []RPCEndpoint
}

// NewSimulatedNetwork initializes an N x N fully connected simulated network switch.
func NewSimulatedNetwork(nodesCount int) *SimulatedNetwork {
	matrix := make([][]bool, nodesCount)
	for i := range nodesCount {
		matrix[i] = make([]bool, nodesCount)
		for j := range nodesCount {
			matrix[i][j] = true
		}
	}

	return &SimulatedNetwork{
		nodesCount: nodesCount,
		connected:  matrix,
		dropRate:   0.0,
		delay:      0,
		endpoints:  make([]RPCEndpoint, nodesCount),
	}
}

// RegisterEndpoint registers an RPCEndpoint handler for a given node ID.
func (sn *SimulatedNetwork) RegisterEndpoint(id int, ep RPCEndpoint) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if id >= 0 && id < sn.nodesCount {
		sn.endpoints[id] = ep
	}
}

// CanCommunicate checks if Node A can reach Node B.
func (sn *SimulatedNetwork) CanCommunicate(from, to int) bool {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if from < 0 || from >= sn.nodesCount || to < 0 || to >= sn.nodesCount {
		return false
	}
	return sn.connected[from][to]
}

// Partition dynamically isolates groupA from groupB.
func (sn *SimulatedNetwork) Partition(groupA, groupB []int) {
	sn.mu.Lock()
	defer sn.mu.Unlock()

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
			sn.connected[a][b] = false
			sn.connected[b][a] = false
		}
	}
}

// Heal restores full mesh connectivity across all nodes.
func (sn *SimulatedNetwork) Heal() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	for i := 0; i < sn.nodesCount; i++ {
		for j := 0; j < sn.nodesCount; j++ {
			sn.connected[i][j] = true
		}
	}
}

// Delay injects synthetic latency into every dispatched RPC.
func (sn *SimulatedNetwork) Delay(d time.Duration) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.delay = d
}

// DropPercentage configures a probabilistic packet drop rate in [0.0, 1.0].
func (sn *SimulatedNetwork) DropPercentage(rate float64) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if rate < 0.0 {
		rate = 0.0
	} else if rate > 1.0 {
		rate = 1.0
	}
	sn.dropRate = rate
}

func (sn *SimulatedNetwork) shouldDeliver(from, to int) bool {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if !sn.connected[from][to] {
		return false
	}
	if sn.dropRate > 0.0 && rand.Float64() < sn.dropRate {
		return false
	}
	return true
}

// CallRequestVote delivers a RequestVote RPC through the simulated network switch.
func (sn *SimulatedNetwork) CallRequestVote(from, to int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if !sn.shouldDeliver(from, to) {
		return false
	}

	sn.mu.RLock()
	delay := sn.delay
	target := sn.endpoints[to]
	sn.mu.RUnlock()

	if target == nil {
		return false
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	target.RequestVote(args, reply)
	return sn.shouldDeliver(to, from)
}

// CallAppendEntries delivers an AppendEntries RPC through the simulated network switch.
func (sn *SimulatedNetwork) CallAppendEntries(from, to int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if !sn.shouldDeliver(from, to) {
		return false
	}

	sn.mu.RLock()
	delay := sn.delay
	target := sn.endpoints[to]
	sn.mu.RUnlock()

	if target == nil {
		return false
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	target.AppendEntries(args, reply)
	return sn.shouldDeliver(to, from)
}

// CallInstallSnapshot delivers an InstallSnapshot RPC through the simulated network switch.
func (sn *SimulatedNetwork) CallInstallSnapshot(from, to int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if !sn.shouldDeliver(from, to) {
		return false
	}

	sn.mu.RLock()
	delay := sn.delay
	target := sn.endpoints[to]
	sn.mu.RUnlock()

	if target == nil {
		return false
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	target.InstallSnapshot(args, reply)
	return sn.shouldDeliver(to, from)
}

