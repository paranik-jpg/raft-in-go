package network

// RequestVoteArgs defines the arguments sent by candidates to gather votes (§5.2, §5.4.1).
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

// LogEntry represents an individual replicated command within the Raft log.
type LogEntry struct {
	Index   int         // 1-based index in the overall replicated log sequence.
	Term    int         // Term number when entry was received by the leader.
	Command interface{} // Application-level command to apply to the state machine.
}

// AppendEntriesArgs defines the arguments sent by the leader for heartbeats and replication (§5.2, §5.3).
type AppendEntriesArgs struct {
	Term         int        // Leader's current term.
	LeaderID     int        // Identifier of leader so follower can redirect clients.
	PrevLogIndex int        // Index of log entry immediately preceding new ones.
	PrevLogTerm  int        // Term of PrevLogIndex entry.
	Entries      []LogEntry // Log entries to store (empty for heartbeat).
	LeaderCommit int        // Leader's commitIndex.
}

// AppendEntriesReply defines the response sent back by peers.
type AppendEntriesReply struct {
	Term          int  // CurrentTerm, for leader to update itself if obsolete.
	Success       bool // True if follower contained entry matching PrevLogIndex and PrevLogTerm.
	ConflictIndex int  // First index of the conflicting term (or log length + 1 if too short).
	ConflictTerm  int  // Term of the conflicting entry (0 if follower log is too short).
}

