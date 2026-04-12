package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketData = []byte("data") // stores Entry values keyed by entry.Key
	bucketMeta = []byte("meta") // stores global metadata (revision counter)
	keyRev     = []byte("rev")  // key inside bucketMeta for the global revision
)

// BoltStore implements Store using BoltDB.
//
// Design notes (interview talking points):
//   - BoltDB provides serializable transactions via a single-writer model.
//     This means the revision counter is naturally serialized — no need for
//     CAS loops or external locks.
//   - In Paladin's production codebase, BoltDB sits behind the Raft FSM.
//     Raft guarantees that only the Leader writes, so the single-writer model
//     of BoltDB aligns perfectly with Raft's guarantees.
type BoltStore struct {
	mu  sync.RWMutex
	db  *bolt.DB
	rev uint64 // cached in memory for fast reads
}

// NewBoltStore opens (or creates) a BoltDB-backed store at the given path.
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	// Ensure buckets exist.
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketData); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketMeta); err != nil {
			return err
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}

	// Load current revision from disk.
	var rev uint64
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if v := b.Get(keyRev); v != nil {
			rev = binary.BigEndian.Uint64(v)
		}
		return nil
	})

	return &BoltStore{db: db, rev: rev}, nil
}

// Put creates or updates a key, bumping the global revision.
func (s *BoltStore) Put(key string, value []byte) (*PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result PutResult

	err := s.db.Update(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketData)
		meta := tx.Bucket(bucketMeta)

		// Read previous entry if exists.
		var prev *Entry
		if raw := data.Get([]byte(key)); raw != nil {
			prev = &Entry{}
			if err := json.Unmarshal(raw, prev); err != nil {
				return fmt.Errorf("unmarshal prev entry: %w", err)
			}
		}

		// Bump global revision.
		newRev := s.rev + 1

		entry := &Entry{
			Key:      key,
			Value:    value,
			Revision: newRev,
		}

		if prev != nil {
			// Update existing key.
			entry.CreateRevision = prev.CreateRevision
			entry.ModRevision = newRev
			entry.Version = prev.Version + 1
			result.PrevEntry = prev
		} else {
			// First creation.
			entry.CreateRevision = newRev
			entry.ModRevision = newRev
			entry.Version = 1
		}

		encoded, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal entry: %w", err)
		}

		if err := data.Put([]byte(key), encoded); err != nil {
			return err
		}

		// Persist new revision.
		var revBuf [8]byte
		binary.BigEndian.PutUint64(revBuf[:], newRev)
		if err := meta.Put(keyRev, revBuf[:]); err != nil {
			return err
		}

		s.rev = newRev
		result.Entry = entry
		return nil
	})

	return &result, err
}

// Get retrieves a key.
func (s *BoltStore) Get(key string) (*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entry Entry
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketData)
		raw := b.Get([]byte(key))
		if raw == nil {
			return ErrKeyNotFound
		}
		return json.Unmarshal(raw, &entry)
	})
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// Delete removes a key.
func (s *BoltStore) Delete(key string) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted Entry

	err := s.db.Update(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketData)
		meta := tx.Bucket(bucketMeta)

		raw := data.Get([]byte(key))
		if raw == nil {
			return ErrKeyNotFound
		}
		if err := json.Unmarshal(raw, &deleted); err != nil {
			return err
		}

		if err := data.Delete([]byte(key)); err != nil {
			return err
		}

		// Bump revision even for deletes — Watch needs to see delete events.
		newRev := s.rev + 1
		var revBuf [8]byte
		binary.BigEndian.PutUint64(revBuf[:], newRev)
		if err := meta.Put(keyRev, revBuf[:]); err != nil {
			return err
		}

		s.rev = newRev
		deleted.Revision = newRev
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &deleted, nil
}

// List returns all entries matching the given prefix.
// Uses BoltDB's cursor to efficiently scan the key range.
func (s *BoltStore) List(prefix string) ([]*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries []*Entry

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketData)
		c := b.Cursor()

		prefixBytes := []byte(prefix)
		for k, v := c.Seek(prefixBytes); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			var entry Entry
			if err := json.Unmarshal(v, &entry); err != nil {
				return err
			}
			entries = append(entries, &entry)
		}
		return nil
	})

	return entries, err
}

// Rev returns the current global revision.
func (s *BoltStore) Rev() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rev
}

// Close closes the underlying BoltDB.
func (s *BoltStore) Close() error {
	return s.db.Close()
}
