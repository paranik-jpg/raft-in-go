package storage

import (
	"raft-go/internal/network"
)

// Storage defines the interface for durable Raft state and log persistence.
// Corresponds to §5.2 "Persistent state on all servers":
// - currentTerm
// - votedFor
// - log[]
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
}

// MemoryStorage provides an in-memory implementation of the Storage interface.
type MemoryStorage struct {
	currentTerm int
	votedFor    int
	log         []network.LogEntry
}

// NewMemoryStorage initializes an in-memory storage instance with sentinel entry at index 0.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		currentTerm: 0,
		votedFor:    -1,
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
