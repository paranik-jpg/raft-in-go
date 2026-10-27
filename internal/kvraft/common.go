package kvraft

// OpType represents the mutation or query operation type.
type OpType string

const (
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
	OpGet    OpType = "Get"
)

// Err represents operation status errors.
type Err string

const (
	OK               Err = "OK"
	ErrNoKey         Err = "ErrNoKey"
	ErrWrongLeader   Err = "ErrWrongLeader"
	ErrTimeout       Err = "ErrTimeout"
	ErrServerStopped Err = "ErrServerStopped" // Explicit status when the node is shut down
)

// Op is the replicated command payload stored in Raft logs.
type Op struct {
	Type      OpType // Put, Append, Get
	Key       string // Key to mutate or read
	Value     string // Value for Put/Append (empty for Get)
	ClientID  int64  // Unique client identifier for linearizability
	SeqNumber int64  // Monotonically increasing per-client request sequence number
}

// PutAppendArgs defines arguments sent by the client Clerk for Put and Append.
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        OpType // Put or Append
	ClientID  int64
	SeqNumber int64
}

// PutAppendReply defines the response sent back to the client Clerk.
type PutAppendReply struct {
	Err Err
}

// GetArgs defines arguments sent by the client Clerk for Get.
type GetArgs struct {
	Key       string
	ClientID  int64
	SeqNumber int64
}

// GetReply defines the response sent back to the client Clerk.
type GetReply struct {
	Err   Err
	Value string
}

