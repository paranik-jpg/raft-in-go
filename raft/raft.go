package raft

import (
	"sync"
	"time"
)

// Timer constants per Raft specification:
// - Election timeout randomized in [150ms, 300ms] to avoid split votes.
// - Heartbeat interval ~50ms (Heartbeat << Election timeout to maintain stable leadership).
const (
	MinElectionTimeout = 150 * time.Millisecond
	MaxElectionTimeout = 300 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond
)

// NodeRole represents the current role of a Raft consensus node.
type NodeRole string

const (
	RoleFollower  NodeRole = "Follower"
	RoleCandidate NodeRole = "Candidate"
	RoleLeader    NodeRole = "Leader"
)

// Raft encapsulates all persistent and volatile state, synchronization
// primitives, and metadata required for a single consensus node.
type Raft struct {
	// mu protects all concurrent read/write access to this node's internal state.
	// Raft nodes receive concurrent RPC requests (AppendEntries, RequestVote) while
	// simultaneously running background timers and client request handlers.
	mu sync.Mutex

	// me is the identifier of this server instance within the cluster.
	me int

	// =========================================================================
	// PERSISTENT STATE ON ALL SERVERS (Updated on stable storage before responding to RPCs)
	// =========================================================================

	// currentTerm is the latest term server has seen.
	// - Initialized to 0 on first boot.
	// - Increases monotonically across the lifetime of the cluster.
	// - If a server receives a request or response with term T > currentTerm,
	//   it MUST update currentTerm = T and convert to Follower.
	currentTerm int

	// votedFor tracks the candidateId that received this server's vote in currentTerm.
	// - Initialized to -1 (indicating none/nil) on boot or term advancement.
	// - A server can vote for at most one candidate per term (Election Safety).
	votedFor int

	// log contains log entries.
	// - In Raft, log entries are 1-indexed.
	// - To simplify index arithmetic and avoid off-by-one errors, log[0] is
	//   initialized with a dummy sentinel entry (Index: 0, Term: 0, Command: nil).
	// - Therefore, log[i].Index == i holds true for all valid entries i >= 1
	//   (until snapshot compaction is introduced in later steps).
	log []LogEntry

	// role tracks whether this node is currently a Follower, Candidate, or Leader.
	role NodeRole

	// =========================================================================
	// VOLATILE STATE ON ALL SERVERS (Reinitialized to 0 after crash/reboot)
	// =========================================================================

	// commitIndex is the index of highest log entry known to be committed.
	// - A log entry is "committed" once it is safely stored on a majority quorum.
	// - Initialized to 0 on boot, increases monotonically.
	commitIndex int

	// lastApplied is the index of highest log entry applied to the state machine.
	// - An entry can only be applied AFTER it is committed (lastApplied <= commitIndex).
	// - Initialized to 0 on boot, increases monotonically.
	lastApplied int

	// =========================================================================
	// VOLATILE STATE ON LEADERS ONLY (Reinitialized after each election)
	// =========================================================================

	// nextIndex tracks, for each peer, the log index of the next entry the leader
	// should send to that peer.
	// - Initialized to leader's last log index + 1 upon winning an election.
	// - If replication fails due to log inconsistency, nextIndex is decremented.
	nextIndex []int

	// matchIndex tracks, for each peer, the index of highest log entry known
	// to be replicated on that peer.
	// - Initialized to 0 on election win, increases monotonically.
	// - Used to compute commitIndex (quorum agreement).
	matchIndex []int

	// peersCount is the total number of nodes in the cluster.
	peersCount int

	// peers is the network or RPC abstraction for communicating with cluster peers.
	// In production, peers[i] invokes an RPC client. In tests, it routes directly.
	peers []*Raft

	// =========================================================================
	// CONCURRENCY & LIFECYCLE CHANNELS
	// =========================================================================

	// stopCh is closed when the Raft instance is killed/stopped.
	stopCh chan struct{}

	// heartbeatCh signals that the node received a valid heartbeat/AppendEntries from a leader.
	heartbeatCh chan struct{}

	// grantVoteCh signals that the node granted its vote to a candidate in RequestVote.
	// Raft rules state it should reset its election timer so it doesn't immediately start a competing election.
	grantVoteCh chan struct{}

	// winElectionCh signals that the candidate successfully collected a majority of votes.
	// To break out of the election wait loop and immediately step up as the Leader.
	winElectionCh chan struct{}
}

// =============================================================================
// FINITE STATE MACHINE (FSM) STATE TRANSITIONS
// All transition methods MUST be called with rf.mu locked.
// =============================================================================

// becomeFollower transitions the node to the Follower role in the given term.
// Raft rule: If any node sees a term T > currentTerm, it MUST update its term to T
// and step down to Follower, clearing votedFor.
func (rf *Raft) becomeFollower(term int) {
	rf.role = RoleFollower
	rf.currentTerm = term
	rf.votedFor = -1 // Reset vote for the new term
}

// becomeCandidate transitions the node to Candidate role, bumping currentTerm
// and voting for itself.
// Corresponds to Initiating the Election steps:
// 1. Increment currentTerm
// 2. Transition state to Candidate
// 3. Vote for self (votedFor = selfID)
func (rf *Raft) becomeCandidate() {
	rf.currentTerm++        // 1. Increment currentTerm
	rf.role = RoleCandidate // 2. Transition state to Candidate
	rf.votedFor = rf.me     // 3. Vote for self (votedFor = selfID)
}

// becomeLeader transitions the node to Leader role and initializes volatile leader state.
func (rf *Raft) becomeLeader() {
	rf.role = RoleLeader
	rf.initializeLeaderVolatileState()
}

// initializeLeaderVolatileState sets up nextIndex and matchIndex when this node becomes leader.
func (rf *Raft) initializeLeaderVolatileState() {
	rf.nextIndex = make([]int, rf.peersCount)
	rf.matchIndex = make([]int, rf.peersCount)

	lastLogIndex := rf.getLastLogIndex()

	for peer := 0; peer < rf.peersCount; peer++ {
		// Initialized to leader's last log index + 1
		rf.nextIndex[peer] = lastLogIndex + 1
		// Initialized to 0, increases monotonically
		rf.matchIndex[peer] = 0
	}
}

// GetRole returns the current role of the node (thread-safe).
func (rf *Raft) GetRole() NodeRole {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

// GetCurrentTerm returns the current term of the node (thread-safe).
func (rf *Raft) GetCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

// SetPeers configures the peer instances for RPC communication.
func (rf *Raft) SetPeers(peers []*Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.peersCount = len(peers)
}

// Stop safely shuts down the Raft node lifecycle loop.
func (rf *Raft) Stop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	select {
	case <-rf.stopCh:
		// Already stopped
		return
	default:
		close(rf.stopCh)
	}
}

// sendSignal sends a non-blocking notification on a channel.
// Non-blocking avoids deadlocking if the receiver isn't currently selecting on the channel.
func sendSignal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// NewRaft creates and initializes the foundational state for a Raft node.
func NewRaft(me int, peersCount int) *Raft {
	rf := &Raft{
		me:            me,
		peersCount:    peersCount,
		currentTerm:   0,
		votedFor:      -1, // -1 denotes no vote cast yet in currentTerm
		role:          RoleFollower,
		commitIndex:   0, // volatile: no entries known to be committed initially
		lastApplied:   0, // volatile: no entries applied to state machine initially
		nextIndex:     nil,
		matchIndex:    nil,
		stopCh:        make(chan struct{}),
		heartbeatCh:   make(chan struct{}, 1),
		grantVoteCh:   make(chan struct{}, 1),
		winElectionCh: make(chan struct{}, 1),
		log: []LogEntry{
			{
				Index:   0,
				Term:    0,
				Command: nil,
			},
		},
	}

	return rf
}
