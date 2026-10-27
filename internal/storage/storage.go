package storage

import (
	"raft-go/internal/network"
)

// StateMachine defines the serialization contract for applications running atop Raft (§7).
// Allows the replicated state store (e.g. KV-Store) to snapshot its in-memory data
// and restore from snapshots during compaction or node recovery.
type StateMachine interface {
	// Snapshot serializes the current point-in-time state machine data into bytes.
	// The consensus engine call this method periodically!
	Snapshot() ([]byte, error)

	// Restore replaces the state machine's internal state with the provided snapshot bytes.
	Restore(data []byte) error
}

// Storage defines the interface for durable Raft state, log persistence, and snapshots.
// Corresponds to §5.2 "Persistent state on all servers" and §7 "Log compaction".
type Storage interface {
	// SaveState persists currentTerm and votedFor atomically.
	SaveState(term int, votedFor int) error

	// ReadState recovers currentTerm and votedFor.
	ReadState() (term int, votedFor int, err error)

	// AppendEntries appends log entries to durable storage.
	AppendEntries(entries []network.LogEntry) error

	// TruncateLog removes entries from index onwards (used on conflict resolution).
	TruncateLog(fromIndex int) error

	// GetEntries returns log entries starting from startIndex.
	GetEntries(startIndex int) ([]network.LogEntry, error)

	// LastLogInfo returns the index and term of the last entry in the log.
	LastLogInfo() (lastIndex int, lastTerm int, err error)

	// SaveSnapshot saves the snapshot payload alongside its log compaction metadata.
	SaveSnapshot(snapshot []byte, lastIncludedIndex int, lastIncludedTerm int) error

	// ReadSnapshot recovers the latest snapshot payload and compaction metadata.
	ReadSnapshot() (snapshot []byte, lastIncludedIndex int, lastIncludedTerm int, err error)
}

// MemoryStorage provides an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	currentTerm int
	votedFor    int
	log         []network.LogEntry
	snapshot    []byte
	// compaction metadata:
	// The highest log index encapsulated inside the snapshot.
	lastIncludedIndex int
	// The term of that highest entry.
	lastIncludedTerm int
}

// NewMemoryStorage initializes an in-memory storage instance with sentinel entry at index 0.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		currentTerm:       0,
		votedFor:          -1,
		snapshot:          nil,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		log: []network.LogEntry{
			{
				Index:   0,
				Term:    0,
				Command: nil,
			},
		},
	}
}

func (ms *MemoryStorage) SaveState(term int, votedFor int) error {
	ms.currentTerm = term
	ms.votedFor = votedFor
	return nil
}

func (ms *MemoryStorage) ReadState() (int, int, error) {
	return ms.currentTerm, ms.votedFor, nil
}

func (ms *MemoryStorage) AppendEntries(entries []network.LogEntry) error {
	ms.log = append(ms.log, entries...)
	return nil
}

func (ms *MemoryStorage) TruncateLog(fromIndex int) error {
	if fromIndex < len(ms.log) {
		ms.log = ms.log[:fromIndex]
	}
	return nil
}

func (ms *MemoryStorage) GetEntries(startIndex int) ([]network.LogEntry, error) {
	if startIndex >= len(ms.log) {
		return nil, nil
	}
	res := make([]network.LogEntry, len(ms.log[startIndex:]))
	copy(res, ms.log[startIndex:])
	return res, nil
}

func (ms *MemoryStorage) LastLogInfo() (int, int, error) {
	last := ms.log[len(ms.log)-1]
	return last.Index, last.Term, nil
}

func (ms *MemoryStorage) SaveSnapshot(snapshot []byte, lastIncludedIndex int, lastIncludedTerm int) error {
	ms.snapshot = make([]byte, len(snapshot))
	copy(ms.snapshot, snapshot)
	ms.lastIncludedIndex = lastIncludedIndex
	ms.lastIncludedTerm = lastIncludedTerm
	return nil
}

func (ms *MemoryStorage) ReadSnapshot() ([]byte, int, int, error) {
	if ms.snapshot == nil {
		return nil, 0, 0, nil
	}
	res := make([]byte, len(ms.snapshot))
	copy(res, ms.snapshot)
	return res, ms.lastIncludedIndex, ms.lastIncludedTerm, nil
}
