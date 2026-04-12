package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultWatchTimeout = 30 * time.Second
	maxWatchTimeout     = 60 * time.Second
)

// WatchResponse wraps a list of events with metadata.
type WatchResponse struct {
	Revision uint64         `json:"revision"`
	Events   []*WatchEvent  `json:"events"`
}

// WatchEvent is the JSON representation of a watch event.
type WatchEvent struct {
	Type      string      `json:"type"`
	Entry     *ConfigItem `json:"entry"`
	PrevEntry *ConfigItem `json:"prev_entry,omitempty"`
}

// RegisterWatchRoutes adds the watch endpoint.
// Called from server.routes() — separated for clarity.
func (s *Server) registerWatchRoutes() {
	s.mux.HandleFunc("/api/v1/watch/", s.handleWatch)
}

// handleWatch implements HTTP long polling for configuration changes.
//
// GET /api/v1/watch/{tenant}/{namespace}/?revision=N&timeout=30
//
// Behavior:
//   1. Client sends revision (last known)
//   2. If events exist after that revision → return immediately
//   3. If not → block until events arrive or timeout
//   4. Client receives events, updates local state, sends next request
//      with new revision
//
// This is the exact mechanism behind Paladin SDK V2's long polling.
func (s *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, http.StatusMethodNotAllowed, "only GET allowed")
		return
	}

	// Parse path: /api/v1/watch/{tenant}/{namespace}/
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/v1/watch/")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		httpError(w, http.StatusBadRequest, "tenant is required")
		return
	}

	// Build the prefix to watch.
	prefix := trimmed + "/"

	// Parse revision from query parameter.
	var afterRev uint64
	if revStr := r.URL.Query().Get("revision"); revStr != "" {
		var err error
		afterRev, err = strconv.ParseUint(revStr, 10, 64)
		if err != nil {
			httpError(w, http.StatusBadRequest, "invalid revision: %s", revStr)
			return
		}
	}

	// Parse timeout.
	timeout := defaultWatchTimeout
	if timeoutStr := r.URL.Query().Get("timeout"); timeoutStr != "" {
		secs, err := strconv.Atoi(timeoutStr)
		if err != nil || secs <= 0 {
			httpError(w, http.StatusBadRequest, "invalid timeout: %s", timeoutStr)
			return
		}
		timeout = time.Duration(secs) * time.Second
		if timeout > maxWatchTimeout {
			timeout = maxWatchTimeout
		}
	}

	// Block until events or timeout.
	events := s.wc.WaitForEvents(afterRev, prefix, timeout)

	watchEvents := make([]*WatchEvent, len(events))
	for i, e := range events {
		we := &WatchEvent{
			Type:  e.Type.String(),
			Entry: entryToConfig(e.Entry),
		}
		if e.PrevEntry != nil {
			we.PrevEntry = entryToConfig(e.PrevEntry)
		}
		watchEvents[i] = we
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Paladin-Revision", fmt.Sprintf("%d", s.store.Rev()))
	json.NewEncoder(w).Encode(&WatchResponse{
		Revision: s.store.Rev(),
		Events:   watchEvents,
	})
}
