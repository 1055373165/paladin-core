package store

import (
	"os"
	"testing"
)

func tempStore(t *testing.T) *BoltStore {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "paladin-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	s, err := NewBoltStore(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutAndGet(t *testing.T) {
	s := tempStore(t)

	// Put a new key.
	res, err := s.Put("app/db_host", []byte("10.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Entry.Version != 1 {
		t.Fatalf("expected version 1, got %d", res.Entry.Version)
	}
	if res.Entry.Revision != 1 {
		t.Fatalf("expected revision 1, got %d", res.Entry.Revision)
	}
	if res.PrevEntry != nil {
		t.Fatal("expected no previous entry on first put")
	}

	// Get the key.
	entry, err := s.Get("app/db_host")
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Value) != "10.0.0.1" {
		t.Fatalf("expected 10.0.0.1, got %s", entry.Value)
	}

	// Update the key.
	res2, err := s.Put("app/db_host", []byte("10.0.0.2"))
	if err != nil {
		t.Fatal(err)
	}
	if res2.Entry.Version != 2 {
		t.Fatalf("expected version 2, got %d", res2.Entry.Version)
	}
	if res2.Entry.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", res2.Entry.Revision)
	}
	if res2.PrevEntry == nil || string(res2.PrevEntry.Value) != "10.0.0.1" {
		t.Fatal("expected previous entry with old value")
	}
	// CreateRevision should stay at 1 across updates.
	if res2.Entry.CreateRevision != 1 {
		t.Fatalf("expected create_revision 1, got %d", res2.Entry.CreateRevision)
	}
}

func TestGetNotFound(t *testing.T) {
	s := tempStore(t)

	_, err := s.Get("nonexistent")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := tempStore(t)

	s.Put("temp/key", []byte("value"))

	deleted, err := s.Delete("temp/key")
	if err != nil {
		t.Fatal(err)
	}
	if string(deleted.Value) != "value" {
		t.Fatalf("expected deleted value 'value', got '%s'", deleted.Value)
	}

	// Revision should have bumped to 2 (put=1, delete=2).
	if s.Rev() != 2 {
		t.Fatalf("expected rev 2, got %d", s.Rev())
	}

	// Get should now fail.
	_, err = s.Get("temp/key")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	s := tempStore(t)

	_, err := s.Delete("nonexistent")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestListPrefix(t *testing.T) {
	s := tempStore(t)

	s.Put("app/db_host", []byte("10.0.0.1"))
	s.Put("app/db_port", []byte("3306"))
	s.Put("app/redis_host", []byte("10.0.0.2"))
	s.Put("other/key", []byte("ignored"))

	entries, err := s.List("app/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries with prefix 'app/', got %d", len(entries))
	}

	// Verify BoltDB returns keys in sorted order.
	if entries[0].Key != "app/db_host" {
		t.Fatalf("expected first key 'app/db_host', got '%s'", entries[0].Key)
	}
}

func TestRevisionMonotonic(t *testing.T) {
	s := tempStore(t)

	for i := 0; i < 100; i++ {
		s.Put("counter", []byte("v"))
	}

	if s.Rev() != 100 {
		t.Fatalf("expected rev 100, got %d", s.Rev())
	}
}

func TestRevisionPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/paladin.db"

	// Write some data.
	s1, _ := NewBoltStore(dbPath)
	s1.Put("k1", []byte("v1"))
	s1.Put("k2", []byte("v2"))
	s1.Close()

	// Reopen — revision should be restored from disk.
	s2, _ := NewBoltStore(dbPath)
	defer s2.Close()

	if s2.Rev() != 2 {
		t.Fatalf("expected rev 2 after reopen, got %d", s2.Rev())
	}

	// New puts should continue from rev 3.
	res, _ := s2.Put("k3", []byte("v3"))
	if res.Entry.Revision != 3 {
		t.Fatalf("expected rev 3, got %d", res.Entry.Revision)
	}
}
