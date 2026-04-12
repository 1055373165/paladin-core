package store

// WatchableStore wraps a BoltStore and a WatchCache to provide
// both KV operations and event streaming.
//
// Every mutation (Put/Delete) is written to the BoltStore first,
// then an Event is appended to the WatchCache.
//
// In Day 4 (Raft), this becomes: Raft Apply → FSM → BoltStore → WatchCache.
type WatchableStore struct {
	*BoltStore
	wc *WatchCache
}

const defaultWatchCacheSize = 4096

// NewWatchableStore wraps a BoltStore with watch capabilities.
func NewWatchableStore(bs *BoltStore) *WatchableStore {
	return &WatchableStore{
		BoltStore: bs,
		wc:        NewWatchCache(defaultWatchCacheSize),
	}
}

// WatchCache returns the underlying WatchCache for direct access
// (used by the HTTP handler for long polling).
func (ws *WatchableStore) WatchCache() *WatchCache {
	return ws.wc
}

// Put overrides BoltStore.Put to also emit a watch event.
func (ws *WatchableStore) Put(key string, value []byte) (*PutResult, error) {
	result, err := ws.BoltStore.Put(key, value)
	if err != nil {
		return nil, err
	}

	ws.wc.Append(Event{
		Type:      EventPut,
		Entry:     result.Entry,
		PrevEntry: result.PrevEntry,
	})

	return result, nil
}

// Delete overrides BoltStore.Delete to also emit a watch event.
func (ws *WatchableStore) Delete(key string) (*Entry, error) {
	deleted, err := ws.BoltStore.Delete(key)
	if err != nil {
		return nil, err
	}

	ws.wc.Append(Event{
		Type:  EventDelete,
		Entry: deleted,
	})

	return deleted, nil
}

// Close closes both the watch cache and the underlying store.
func (ws *WatchableStore) Close() error {
	ws.wc.Close()
	return ws.BoltStore.Close()
}
