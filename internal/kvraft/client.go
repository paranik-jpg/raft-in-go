package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

// Clerk represents an RPC client communicating with the distributed KVServer cluster.
// It enforces linearizable at-most-once semantics by attaching a unique (ClientID, SeqNumber)
// to every request and automatically retrying across server re-elections.
type Clerk struct {
	mu        sync.Mutex
	servers   []*KVServer
	clientID  int64
	seqNumber int64
	leaderID  int // Cache
}

// nrand generates a cryptographically strong random int64 client identifier.
func nrand() int64 {
	max := new(big.Int).Lsh(big.NewInt(1), 62)
	n, _ := rand.Int(rand.Reader, max)
	return n.Int64()
}

// NewClerk creates and initializes a client instance connected to servers.
func NewClerk(servers []*KVServer) *Clerk {
	ck := &Clerk{
		servers:   servers,
		clientID:  nrand(),
		seqNumber: 0,
		leaderID:  0,
	}
	return ck
}

// Get fetches the current value for a key from the replicated state machine.
// Returns empty string if the key does not exist.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	seq := atomic.AddInt64(&ck.seqNumber, 1)
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		SeqNumber: seq,
	}
	leader := ck.leaderID
	servers := ck.servers
	ck.mu.Unlock()

	for {
		reply := GetReply{}
		server := servers[leader]
		server.Get(&args, &reply)

		if reply.Err == OK {
			ck.mu.Lock()
			ck.leaderID = leader
			ck.mu.Unlock()
			return reply.Value
		}

		if reply.Err == ErrNoKey {
			ck.mu.Lock()
			ck.leaderID = leader
			ck.mu.Unlock()
			return ""
		}

		// ErrWrongLeader, ErrTimeout, or ErrServerStopped: try next server in cluster
		leader = (leader + 1) % len(servers)
		time.Sleep(10 * time.Millisecond)
	}
}

// GetLinearizable fetches the current value with guaranteed linearizability using ReadIndex (§8).
func (ck *Clerk) GetLinearizable(key string) string {
	ck.mu.Lock()
	seq := atomic.AddInt64(&ck.seqNumber, 1)
	args := GetArgs{
		Key:       key,
		ClientID:  ck.clientID,
		SeqNumber: seq,
	}
	leader := ck.leaderID
	servers := ck.servers
	ck.mu.Unlock()

	for {
		reply := GetReply{}
		server := servers[leader]
		server.GetLinearizable(&args, &reply)

		if reply.Err == OK {
			ck.mu.Lock()
			ck.leaderID = leader
			ck.mu.Unlock()
			return reply.Value
		}

		if reply.Err == ErrNoKey {
			ck.mu.Lock()
			ck.leaderID = leader
			ck.mu.Unlock()
			return ""
		}

		leader = (leader + 1) % len(servers)
		time.Sleep(10 * time.Millisecond)
	}
}

// GetLocal executes a naive in-memory read against the current leader without consulting quorum.
func (ck *Clerk) GetLocal(key string) string {
	ck.mu.Lock()
	leader := ck.leaderID
	servers := ck.servers
	ck.mu.Unlock()

	val, _ := servers[leader].GetLocal(key)
	return val
}


// Put sets the value for a key in the replicated state machine.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}

// Append appends value to an existing key, or creates it if non-existent.
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}

// PutAppend handles Put and Append mutations with at-most-once retry loop.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	ck.mu.Lock()
	seq := atomic.AddInt64(&ck.seqNumber, 1)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		SeqNumber: seq,
	}
	leader := ck.leaderID
	servers := ck.servers
	ck.mu.Unlock()

	for {
		reply := PutAppendReply{}
		server := servers[leader]
		server.PutAppend(&args, &reply)

		if reply.Err == OK {
			ck.mu.Lock()
			ck.leaderID = leader
			ck.mu.Unlock()
			return
		}

		// ErrWrongLeader, ErrTimeout, or ErrServerStopped: advance to next server
		leader = (leader + 1) % len(servers)
		time.Sleep(10 * time.Millisecond)
	}
}
