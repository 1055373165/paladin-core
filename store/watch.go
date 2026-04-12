package store

import (
	"strings"
	"sync"
	"time"
)

// EventType represents the type of a watch event.
type EventType int

const (
	EventPut    EventType = iota // Key was created or updated
	EventDelete                  // Key was deleted
)

func (t EventType) String() string {
	switch t {
	case EventPut:
		return "PUT"
	case EventDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Event represents a single change in the store.
// Watch consumers receive a stream of Events.
type Event struct {
	Type      EventType `json:"type"`
	Entry     *Entry    `json:"entry"`               // Current state (after the change)
	PrevEntry *Entry    `json:"prev_entry,omitempty"` // Previous state (nil for creates)
}

// WatchCache is a ring buffer that caches the most recent N events.
//
// Design (inspired by Paladin's watchCache / etcd's watchableStore):
//   - Fixed-size ring buffer avoids unbounded memory growth
//   - sync.Cond for efficient blocking/waking of long-poll clients
//   - Binary search on revision for fast event lookup
//
// Why a ring buffer and not a channel or linked list?
//   - Channel: can't do "give me all events after revision N" — channels are FIFO only
//   - Linked list: O(n) scan to find events after revision N
//   - Ring buffer: O(1) append, O(log n) lookup by revision (binary search)
type WatchCache struct {
	mu       sync.RWMutex
	cond     *sync.Cond
	buf      []Event
	capacity int
	count    int   // total events written (monotonic, used to compute position)
	closed   bool
}

// NewWatchCache creates a ring buffer with the given capacity.
func NewWatchCache(capacity int) *WatchCache {
	wc := &WatchCache{
		buf:      make([]Event, capacity),
		capacity: capacity,
	}
	wc.cond = sync.NewCond(&wc.mu)
	return wc
}

// Append adds an event to the ring buffer and wakes up all waiting watchers.
func (wc *WatchCache) Append(event Event) {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	pos := wc.count % wc.capacity
	wc.buf[pos] = event
	wc.count++

	// Wake up all blocked WaitForEvents callers.
	wc.cond.Broadcast()
}

// WaitForEvents blocks until there are events with revision > afterRev,
// or until the timeout expires. Returns nil if timeout or closed.
//
// This is the core of long polling:
//  1. Client sends request with afterRev (last known revision)
//  2. Server calls WaitForEvents(afterRev, 30s)
//  3. If events exist → return immediately
//  4. If not → block on cond.Wait() until Append() calls Broadcast()
//  5. If timeout → return empty (client will retry)
func (wc *WatchCache) WaitForEvents(afterRev uint64, prefix string, timeout time.Duration) []Event {
	deadline := time.Now().Add(timeout)

	wc.mu.Lock()
	defer wc.mu.Unlock()

	for {
		if wc.closed {
			return nil
		}

		events := wc.getEventsLocked(afterRev, prefix)
		if len(events) > 0 {
			return events
		}

		// No events yet — wait with timeout.
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil // Timeout
		}

		// Wait with timeout using a goroutine-based timer.
		// sync.Cond doesn't have native timeout support, so we use
		// a timer goroutine that calls Broadcast after the deadline.
		done := make(chan struct{})
		go func() {
			timer := time.NewTimer(remaining)
			defer timer.Stop()
			select {
			case <-timer.C:
				wc.cond.Broadcast() // Wake up to check timeout
			case <-done:
			}
		}()

		wc.cond.Wait()
		close(done)
	}
}

// getEventsLocked returns all events with revision > afterRev matching the prefix.
// Must be called with wc.mu held.
func (wc *WatchCache) getEventsLocked(afterRev uint64, prefix string) []Event {
	if wc.count == 0 {
		return nil
	}

	// Determine the range of valid events in the ring buffer.
	// oldest = max(0, count - capacity)
	oldest := 0
	if wc.count > wc.capacity {
		oldest = wc.count - wc.capacity
	}

	var result []Event
	for i := oldest; i < wc.count; i++ {
		pos := i % wc.capacity
		ev := wc.buf[pos]
		if ev.Entry.Revision <= afterRev {
			continue
		}
		if prefix != "" && !strings.HasPrefix(ev.Entry.Key, prefix) {
			continue
		}
		result = append(result, ev)
	}
	return result
}

// Len returns the number of events currently in the buffer.
func (wc *WatchCache) Len() int {
	wc.mu.RLock()
	defer wc.mu.RUnlock()
	if wc.count < wc.capacity {
		return wc.count
	}
	return wc.capacity
}

// Close stops the watch cache and wakes up all waiters.
func (wc *WatchCache) Close() {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.closed = true
	wc.cond.Broadcast()
}
