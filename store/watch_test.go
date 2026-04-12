package store

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestWatchCacheAppendAndGet(t *testing.T) {
	wc := NewWatchCache(10)
	defer wc.Close()

	// Append 3 events.
	for i := uint64(1); i <= 3; i++ {
		wc.Append(Event{
			Type:  EventPut,
			Entry: &Entry{Key: "k", Revision: i},
		})
	}

	// Get events after rev 0 → all 3.
	events := wc.WaitForEvents(0, "", 100*time.Millisecond)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// Get events after rev 2 → only rev 3.
	events = wc.WaitForEvents(2, "", 100*time.Millisecond)
	if len(events) != 1 || events[0].Entry.Revision != 3 {
		t.Fatalf("expected 1 event at rev 3, got %v", events)
	}
}

func TestWatchCacheRingOverflow(t *testing.T) {
	wc := NewWatchCache(5) // Small buffer.
	defer wc.Close()

	// Write 8 events → first 3 should be lost.
	for i := uint64(1); i <= 8; i++ {
		wc.Append(Event{
			Type:  EventPut,
			Entry: &Entry{Key: "k", Revision: i},
		})
	}

	if wc.Len() != 5 {
		t.Fatalf("expected buffer len 5, got %d", wc.Len())
	}

	// Get all → should only return events 4-8.
	events := wc.WaitForEvents(0, "", 100*time.Millisecond)
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	if events[0].Entry.Revision != 4 {
		t.Fatalf("expected oldest event rev=4, got %d", events[0].Entry.Revision)
	}
}

func TestWatchCachePrefixFilter(t *testing.T) {
	wc := NewWatchCache(10)
	defer wc.Close()

	wc.Append(Event{Type: EventPut, Entry: &Entry{Key: "public/prod/db_host", Revision: 1}})
	wc.Append(Event{Type: EventPut, Entry: &Entry{Key: "public/prod/db_port", Revision: 2}})
	wc.Append(Event{Type: EventPut, Entry: &Entry{Key: "public/staging/db_host", Revision: 3}})

	// Watch only "public/prod/" prefix.
	events := wc.WaitForEvents(0, "public/prod/", 100*time.Millisecond)
	if len(events) != 2 {
		t.Fatalf("expected 2 events for prefix 'public/prod/', got %d", len(events))
	}
}

func TestWatchCacheBlocking(t *testing.T) {
	wc := NewWatchCache(10)
	defer wc.Close()

	var result []Event
	var wg sync.WaitGroup
	wg.Add(1)

	// Start a watcher that blocks waiting for events after rev 0.
	go func() {
		defer wg.Done()
		result = wc.WaitForEvents(0, "", 5*time.Second)
	}()

	// Wait a bit, then append an event.
	time.Sleep(100 * time.Millisecond)
	wc.Append(Event{Type: EventPut, Entry: &Entry{Key: "k", Revision: 1}})

	wg.Wait()

	if len(result) != 1 {
		t.Fatalf("expected 1 event, got %d", len(result))
	}
}

func TestWatchCacheTimeout(t *testing.T) {
	wc := NewWatchCache(10)
	defer wc.Close()

	start := time.Now()
	events := wc.WaitForEvents(0, "", 200*time.Millisecond)
	elapsed := time.Since(start)

	if len(events) != 0 {
		t.Fatalf("expected 0 events on timeout, got %d", len(events))
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("returned too early: %v", elapsed)
	}
}

func TestWatchableStorePutEmitsEvent(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "*.db")
	f.Close()
	bs, _ := NewBoltStore(f.Name())
	ws := NewWatchableStore(bs)
	defer ws.Close()

	ws.Put("public/prod/db_host", []byte("10.0.0.1"))
	ws.Put("public/prod/db_host", []byte("10.0.0.2"))

	events := ws.WatchCache().WaitForEvents(0, "", 100*time.Millisecond)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// First event: create (no prev).
	if events[0].PrevEntry != nil {
		t.Fatal("expected no prev entry on first put")
	}
	// Second event: update (has prev).
	if events[1].PrevEntry == nil || string(events[1].PrevEntry.Value) != "10.0.0.1" {
		t.Fatal("expected prev entry on update")
	}
}

func TestWatchableStoreDeleteEmitsEvent(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "*.db")
	f.Close()
	bs, _ := NewBoltStore(f.Name())
	ws := NewWatchableStore(bs)
	defer ws.Close()

	ws.Put("key", []byte("val"))
	ws.Delete("key")

	events := ws.WatchCache().WaitForEvents(0, "", 100*time.Millisecond)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[1].Type != EventDelete {
		t.Fatalf("expected DELETE event, got %s", events[1].Type)
	}
}
