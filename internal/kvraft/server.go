package kvraft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"raft-go/internal/raft"
)

// OpResult holds the execution outcome of an operation.
type OpResult struct {
	Err   Err
	Value string
}

// KVServer represents a replicated fault-tolerant key-value store running atop Raft.
type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         bool
	stopCh       chan struct{}
	maxRaftState int // Snapshot when log state exceeds this (in bytes or count, -1 to disable)

	// In-memory key-value state store
	kvStore map[string]string // The actual database

	// Duplicate request detection table: clientID -> last applied SeqNumber
	lastAppliedSeq map[int64]int64

	// Waiting client notification channels: log index -> chan OpResult
	notifyCh map[int]chan OpResult // Wakes up waiting client RPC goroutines: logIndex -> channel
}

// NewKVServer initializes a new replicated key-value server node.
func NewKVServer(me int, rf *raft.Raft, applyCh chan raft.ApplyMsg, maxRaftState int) *KVServer {
	gob.Register(Op{})

	kv := &KVServer{
		me:             me,
		rf:             rf,
		applyCh:        applyCh,
		maxRaftState:   maxRaftState,
		stopCh:         make(chan struct{}),
		kvStore:        make(map[string]string),
		lastAppliedSeq: make(map[int64]int64),
		notifyCh:       make(map[int]chan OpResult),
	}

	// If Raft storage already contains a snapshot (e.g. node restart / crash recovery),
	// restore the state machine immediately upon initialization!
	if rf != nil && rf.GetStorage() != nil {
		snapData, _, _, err := rf.GetStorage().ReadSnapshot()
		if err == nil && len(snapData) > 0 {
			kv.restoreSnapshot(snapData)
		}
	}

	go kv.applierLoop()

	return kv
}

// Get handles client read requests.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:      OpGet,
		Key:       args.Key,
		ClientID:  args.ClientID,
		SeqNumber: args.SeqNumber,
	}

	index, _, isLeader := kv.rf.StartCommand(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	// We register ch in the server's notification map:
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	// Happy Path - Quorum Reached & Entry Applied
	case res := <-ch:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	case <-kv.stopCh:
		reply.Err = ErrServerStopped
	}
}

// GetLinearizable executes a verified linearizable read using the Raft ReadIndex protocol (§8).
// Instead of writing a dummy OpGet into the replicated log, the leader verifies its leadership
// authority via an immediate heartbeat round trip with the majority and waits until lastApplied >= readIndex.
func (kv *KVServer) GetLinearizable(args *GetArgs, reply *GetReply) {
	_, ok := kv.rf.ReadIndex()
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exists := kv.kvStore[args.Key]
	if exists {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
}

// GetLocal executes a naive local in-memory read without consulting Raft quorum.
// This is extremely fast (zero network roundtrips) but can serve stale data if the leader is partitioned.
func (kv *KVServer) GetLocal(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exists := kv.kvStore[key]
	return val, exists
}


// PutAppend handles client mutation requests (Put or Append).
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		SeqNumber: args.SeqNumber,
	}

	index, _, isLeader := kv.rf.StartCommand(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan OpResult, 1)
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		reply.Err = res.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	case <-kv.stopCh:
		reply.Err = ErrServerStopped
	}
}

// isDuplicate checks if client's seqNumber has already been executed.
func (kv *KVServer) isDuplicate(clientID int64, seqNumber int64) bool {
	lastSeq, exists := kv.lastAppliedSeq[clientID]
	return exists && seqNumber <= lastSeq
}

// applierLoop reads committed ApplyMsg instances from Raft and applies them to the KV state machine.
func (kv *KVServer) applierLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg, ok := <-kv.applyCh:
			if !ok {
				return
			}

			if msg.SnapshotValid {
				// Restore snapshot image
				kv.mu.Lock()
				kv.restoreSnapshot(msg.Snapshot)
				kv.mu.Unlock()
				continue
			}

			if !msg.CommandValid {
				continue
			}

			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			kv.mu.Lock()
			res := OpResult{Err: OK}

			switch op.Type {
			case OpPut:
				if !kv.isDuplicate(op.ClientID, op.SeqNumber) {
					kv.kvStore[op.Key] = op.Value
					kv.lastAppliedSeq[op.ClientID] = op.SeqNumber
				}
			case OpAppend:
				if !kv.isDuplicate(op.ClientID, op.SeqNumber) {
					kv.kvStore[op.Key] += op.Value
					kv.lastAppliedSeq[op.ClientID] = op.SeqNumber
				}
			case OpGet:
				val, exists := kv.kvStore[op.Key]
				if exists {
					res.Value = val
					res.Err = OK
				} else {
					res.Err = ErrNoKey
				}
			}

			// Notify waiting RPC handler at msg.CommandIndex
			if ch, exists := kv.notifyCh[msg.CommandIndex]; exists {
				select {
				// If the client is still waiting, it receives res instantly and wakes up
				case ch <- res:
				default:
				}
			}

			// Snapshot trigger if maxRaftState threshold is set
			if kv.maxRaftState > 0 && msg.CommandIndex%kv.maxRaftState == 0 {
				// Freeze the current point-in-time state:
				snapData := kv.encodeSnapshot()
				// Hand snapData down to Raft
				kv.rf.Snapshot(msg.CommandIndex, snapData)
			}

			kv.mu.Unlock()
		}
	}
}

// encodeSnapshot serializes current KV map and duplicate table.
// Must be called with kv.mu held.
func (kv *KVServer) encodeSnapshot() []byte {
	// w => a clean byte bucket.
	w := new(bytes.Buffer)
	// Whatever we pass to e.Encode(...) will be serialized into compact binary bytes
	// and pushed directly into w
	e := gob.NewEncoder(w)
	_ = e.Encode(kv.kvStore)
	_ = e.Encode(kv.lastAppliedSeq)
	return w.Bytes()
}

// restoreSnapshot decodes snapshot into in-memory state.
// Must be called with kv.mu held.
func (kv *KVServer) restoreSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var store map[string]string
	var lastSeq map[int64]int64
	if d.Decode(&store) == nil && d.Decode(&lastSeq) == nil {
		kv.kvStore = store
		kv.lastAppliedSeq = lastSeq
	}
}

// Kill terminates the KVServer instance.
func (kv *KVServer) Kill() {
	kv.mu.Lock()
	if !kv.dead {
		kv.dead = true
		close(kv.stopCh)
	}
	kv.mu.Unlock()
}
