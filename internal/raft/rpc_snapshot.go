package raft

// InstallSnapshotArgs defines the arguments sent by the leader to install a snapshot on a lagging peer (§7).
type InstallSnapshotArgs struct {
	Term              int    // Leader's current term.
	LeaderID          int    // Identifier of leader so follower can redirect clients.
	LastIncludedIndex int    // The snapshot replaces all entries up through and including this index.
	LastIncludedTerm  int    // Term of LastIncludedIndex.
	Data              []byte // Raw snapshot bytes of the state machine.
}

// InstallSnapshotReply defines the response sent back by peers to the leader (§7).
type InstallSnapshotReply struct {
	Term int // CurrentTerm, for leader to update itself if obsolete.
}

// InstallSnapshot is the incoming RPC handler invoked by the leader (§7).
// When a follower lags so far behind that its nextIndex is older than the leader's
// earliest compacted log entry, the leader sends an InstallSnapshot RPC instead of AppendEntries.
//
// The Follower Receiver Logic (§7):
// 1. Reply immediately if term < currentTerm.
// 2. If term > currentTerm, update term and demote to Follower.
// 3. Reset local election timer (valid leader contact).
// 4. If snapshot is older than local base offset (already installed), ignore.
// 5. If existing log entry has same index and term as lastIncludedIndex and lastIncludedTerm,
//    retain that entry and log entries following it.
// 6. Otherwise, discard the entire log; reset log[0] sentinel to lastIncludedIndex / lastIncludedTerm.
// 7. Advance commitIndex and lastApplied to max(commitIndex, lastIncludedIndex).
// 8. Deliver ApplyMsg with SnapshotValid = true to applyCh for the state machine.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	// Rule 1: Reject if leader's term is older than our current term
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// Rule 2: If args.Term > currentTerm or we are not a Follower, step down
	if args.Term > rf.currentTerm || rf.role != RoleFollower {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// Rule 3: Valid leader contact recognized -> reset election timer
	sendSignal(rf.heartbeatCh)

	// Rule 4: If snapshot is stale (already incorporated or older than local base), return early
	if args.LastIncludedIndex <= rf.log[0].Index {
		rf.mu.Unlock()
		return
	}

	// Rule 5 & 6: Splicing or discarding the log
	sliceIdx := rf.toSliceIndex(args.LastIncludedIndex)
	if sliceIdx < len(rf.log) && rf.log[sliceIdx].Term == args.LastIncludedTerm {
		// Existing entry matches lastIncludedIndex and lastIncludedTerm!
		// Retain entries following lastIncludedIndex
		newLog := make([]LogEntry, 0, len(rf.log)-sliceIdx)
		newLog = append(newLog, LogEntry{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Command: nil,
		})
		newLog = append(newLog, rf.log[sliceIdx+1:]...)
		rf.log = newLog
	} else {
		// Log does not contain matching entry or is shorter: discard entire log
		rf.log = []LogEntry{
			{
				Index:   args.LastIncludedIndex,
				Term:    args.LastIncludedTerm,
				Command: nil,
			},
		}
	}

	// Rule 7: Update commitIndex and lastApplied up to lastIncludedIndex
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	// Persist snapshot to durable storage if available
	if rf.storage != nil {
		_ = rf.storage.SaveSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	}

	// Rule 8: Deliver snapshot to the state machine via applyCh
	// To avoid deadlock, deliver without holding rf.mu!
	snapMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	applyCh := rf.applyCh
	rf.mu.Unlock()

	// Deliver snapshot message to state machine asynchronously / non-blocking with respect to lock
	if applyCh != nil {
		applyCh <- snapMsg
	}
}

// sendInstallSnapshot dispatches an InstallSnapshot RPC to the specified peer.
func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
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
		return netMatrix.CallInstallSnapshot(from, peer, args, reply)
	}

	targetPeer.InstallSnapshot(args, reply)
	return true
}

// sendInstallSnapshotToPeer is called by the leader when nextIndex[peer] <= log[0].Index (§7).
// It reads the leader's snapshot from storage and dispatches it in a background goroutine.
// Must be called with rf.mu locked; unlocks rf.mu before network dispatch.
func (rf *Raft) sendInstallSnapshotToPeer(peer int) {
	if rf.storage == nil {
		rf.mu.Unlock()
		return
	}

	snapData, lastIndex, lastTerm, err := rf.storage.ReadSnapshot()
	if err != nil || lastIndex == 0 {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	leaderID := rf.me
	rf.mu.Unlock()

	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              snapData,
	}
	reply := &InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Stale response verification
	if rf.role != RoleLeader || rf.currentTerm != term {
		return
	}

	// Higher term encountered: demote to follower immediately
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// Snapshot successfully received by peer:
	// Advance peer's matchIndex and nextIndex to reflect the snapshot installation
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
}
