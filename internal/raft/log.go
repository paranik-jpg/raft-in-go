package raft

// LogEntry represents an individual replicated command within the Raft log.
//
// In Raft, log entries are 1-indexed. Each entry encapsulates:
// - Index: A monotonically increasing 1-based log position.
// - Term: The leader's term when this entry was created.
// - Command: The arbitrary state-machine payload (e.g., Put(k, v), Delete(k)).
type LogEntry struct {
	Index   int // 1-based index in the overall replicated log sequence.
	Term    int // Term number when entry was received by the leader.
	Command any // Application-level command to apply to the state machine.
}

// getLastLogIndex returns the 1-based index of the last entry in the log.
// Must be called with rf.mu locked (or during initialization).
func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// getLastLogTerm returns the term of the last entry in the log.
// Must be called with rf.mu locked (or during initialization).
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// getFirstLogIndex returns the base offset index of the log (lastIncludedIndex of snapshot,
// or 0 if no snapshot has been taken yet).
// Must be called with rf.mu locked.
func (rf *Raft) getFirstLogIndex() int {
	return rf.log[0].Index
}

// toSliceIndex converts a 1-based global Raft log index into the local slice offset.
// Must be called with rf.mu locked.
// This maps the global Raft index to the exact local slot in Go memory.
func (rf *Raft) toSliceIndex(raftIndex int) int {
	return raftIndex - rf.log[0].Index
}

// Snapshot truncates historical log entries up to lastIncludedIndex (§7).
// Preserves lastIncludedIndex and lastIncludedTerm as the new sentinel base offset
// at log[0] for index calculations, releasing compacted entries from memory.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	// If index is already compacted or beyond applied state, do nothing
	if index <= baseIndex || index > rf.lastApplied {
		return
	}

	// This maps the global Raft index to the exact local slot in Go memory.
	sliceIdx := rf.toSliceIndex(index)
	lastIncludedTerm := rf.log[sliceIdx].Term

	// Create compacted log slice with new sentinel at index 0
	// Length = 0, Cap = len(...
	newLog := make([]LogEntry, 0, len(rf.log)-sliceIdx)
	// New sentinal
	newLog = append(newLog, LogEntry{
		Index:   index,
		Term:    lastIncludedTerm,
		Command: nil,
	})
	newLog = append(newLog, rf.log[sliceIdx+1:]...)
	rf.log = newLog

	// Persist snapshot to durable storage if available
	if rf.storage != nil {
		_ = rf.storage.SaveSnapshot(snapshot, index, lastIncludedTerm)
	}
}
