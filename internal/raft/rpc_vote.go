package raft

import (
	"sync"
)

// RequestVoteArgs defines the arguments sent by candidates to gather votes.
// Corresponds strictly to Raft §5.2 and §5.4.1.
type RequestVoteArgs struct {
	Term         int // Candidate's current election term.
	CandidateID  int // Unique identifier of candidate requesting the vote.
	LastLogIndex int // Index of candidate's last log entry.
	LastLogTerm  int // Term of candidate's last log entry.
}

// RequestVoteReply defines the response sent back by peers to the candidate.
type RequestVoteReply struct {
	Term        int  // CurrentTerm of voter, for candidate to update itself if stale.
	VoteGranted bool // True means candidate received vote.
}

// isLogUpToDate implements the Election Restriction invariant (§5.4.1).
// Raft determines which of two logs is more up-to-date by comparing the
// index and term of the last entries in the logs:
//  1. If the logs have last entries with different terms, then the log with the
//     later term is more up-to-date.
//  2. If the logs end with the same term, whichever log is longer is more up-to-date.
//
// Must be called with rf.mu locked.
func (rf *Raft) isLogUpToDate(candidateLastIndex int, candidateLastTerm int) bool {
	myLastTerm := rf.getLastLogTerm()
	myLastIndex := rf.getLastLogIndex()

	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= myLastIndex
}

// RequestVote is the incoming RPC handler invoked on peers by candidates.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Default response: reject vote
	reply.VoteGranted = false

	// Rule 1: If args.Term < currentTerm, reject immediately (§5.1).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Rule 2: If args.Term > currentTerm, update currentTerm and step down (§5.1).
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Always report our currentTerm back to the candidate
	reply.Term = rf.currentTerm

	// Rule 3: Check vote eligibility and log up-to-dateness (§5.2, §5.4.1).
	// Can only vote if:
	// a) We haven't voted for anyone else this term (rf.votedFor == -1 || rf.votedFor == args.CandidateID)
	// b) Candidate's log is at least as up-to-date as our own.
	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateID)
	logOk := rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)

	if canVote && logOk {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true

		// Reset election timer because we granted our vote to a valid candidate (§5.2).
		// This prevents the voter from immediately timing out and interrupting the candidate.
		sendSignal(rf.grantVoteCh)
	}
}

// sendRequestVote dispatches a RequestVote RPC to the specified peer.
// If a NetworkMatrix is configured, it routes through the simulator switch.
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	netMatrix := rf.netMatrix
	from := rf.me
	if peer < 0 || peer >= len(rf.peers) || rf.peers[peer] == nil {
		rf.mu.Unlock()
		return false
	}
	targetPeer := rf.peers[peer]
	rf.mu.Unlock()

	if netMatrix != nil {
		return netMatrix.CallRequestVote(from, peer, args, reply)
	}

	targetPeer.RequestVote(args, reply)
	return true
}

// startElection broadcasts RequestVote RPCs in parallel to all peers.
// It executes the Parallel Vote Fan-Out:
// 1. Snapshot local term and log metadata under rf.mu lock.
// 2. Dispatch RequestVote RPCs to all peers concurrently in dedicated goroutines.
// 3. Collect votes using a thread-safe synchronized counter.
// Must be called with rf.mu locked (caller transitions to Candidate first).
func (rf *Raft) startElection() {
	term := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	// Candidate starts with 1 vote (voted for itself)
	votesCount := 1
	var votesMu sync.Mutex

	// Calculate majority quorum: floor(N / 2) + 1
	quorum := (rf.peersCount / 2) + 1

	// Check if already won (single-node cluster where N=1, quorum=1)
	if votesCount >= quorum {
		sendSignal(rf.winElectionCh)
		return
	}

	// Dispatch RequestVote RPCs to all peers concurrently in dedicated goroutines
	for peer := 0; peer < rf.peersCount; peer++ {
		if peer == rf.me {
			continue // Already voted for self
		}

		go func(p int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			// Dispatch RPC without holding rf.mu (never hold mutex across network calls)
			ok := rf.sendRequestVote(p, args, reply)
			if !ok {
				return
			}

			// Synchronize response handling
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Check for stale response:
			// If our role changed (e.g. heard from a valid leader) or term advanced,
			// discard this response.
			if rf.role != RoleCandidate || rf.currentTerm != term {
				return
			}

			// If the peer's term is higher, we are out-of-date: immediately step down!
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			// If vote was granted, increment vote counter thread-safely
			if reply.VoteGranted {
				votesMu.Lock()
				votesCount++
				won := (votesCount >= quorum)
				votesMu.Unlock()

				// If majority quorum reached, notify run() loop to become Leader
				if won {
					sendSignal(rf.winElectionCh)
				}
			}
		}(peer)
	}
}
