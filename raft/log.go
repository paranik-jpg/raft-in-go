package raft

// LogEntry represents an individual replicated command within the Raft log.
//
// In Raft, log entries are 1-indexed. Each entry encapsulates:
// - Index: A monotonically increasing 1-based log position.
// - Term: The leader's term when this entry was created.
// - Command: The arbitrary state-machine payload (e.g., Put(k, v), Delete(k)).
type LogEntry struct {
	Index   int         // 1-based index in the overall replicated log sequence.
	Term    int         // Term number when entry was received by the leader.
	Command interface{} // Application-level command to apply to the state machine.
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

