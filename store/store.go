// Package store defines the core KV storage interface for PaladinCore.
//
// Day 1: We define the fundamental abstraction that all higher layers build upon.
// Key insight: a configuration center is essentially a versioned KV store.
// The "versioned" part (revision) is what distinguishes it from a plain map.
package store

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyExists   = errors.New("key already exists")
	ErrRevMismatch = errors.New("revision mismatch")
)

// Entry represents a single configuration item in the store.
// Every mutation bumps the global Revision.
//
// Why separate Revision / CreateRevision / ModRevision / Version?
//   - Revision:       global monotonic counter (logical clock for the whole store)
//   - CreateRevision: the Revision when this key was first created
//   - ModRevision:    the Revision when this key was last modified
//   - Version:        how many times this specific key has been modified (starts at 1)
//
// This design comes from etcd and is the foundation of Watch:
// a client says "give me all events after Revision N" — the store looks up
// the global event log and returns everything with Revision > N.
type Entry struct {
	Key            string `json:"key"`
	Value          []byte `json:"value"`
	Revision       uint64 `json:"revision"`
	CreateRevision uint64 `json:"create_revision"`
	ModRevision    uint64 `json:"mod_revision"`
	Version        int64  `json:"version"`
}

// PutResult is returned after a successful Put.
type PutResult struct {
	Entry    *Entry // the entry as written
	PrevEntry *Entry // the previous entry, if any (nil for first create)
}

// Store is the core interface. Day 1 provides a BoltDB implementation.
// Day 4 will wrap this behind a Raft FSM.
type Store interface {
	// Put creates or updates a key. Returns the written entry and the
	// previous entry if one existed.
	Put(key string, value []byte) (*PutResult, error)

	// Get retrieves a single key. Returns ErrKeyNotFound if missing.
	Get(key string) (*Entry, error)

	// Delete removes a key. Returns the deleted entry, or ErrKeyNotFound.
	Delete(key string) (*Entry, error)

	// List returns all entries whose key starts with the given prefix.
	List(prefix string) ([]*Entry, error)

	// Rev returns the current global revision.
	Rev() uint64

	// Close releases all resources.
	Close() error
}
