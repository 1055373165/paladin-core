// Package server - Day 5 additions: Raft-aware HTTP server with
// ForwardRPC (transparent leader proxy) and consistent read options.
package server

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	praft "github.com/smy/paladin-core/raft"
)

// RaftServer extends Server with Raft awareness.
// Follower nodes transparently forward write requests to the Leader.
type RaftServer struct {
	*Server
	node *praft.Node
}

// NewRaftServer creates a Raft-aware HTTP server.
func NewRaftServer(node *praft.Node) *RaftServer {
	// Build the base server with the node's WatchableStore.
	base := New(node.Store())

	rs := &RaftServer{Server: base, node: node}

	// Override config routes to go through Raft.
	rs.mux.HandleFunc("/api/v1/config/", rs.handleRaftConfig)
	// Watch still works the same (reads from WatchCache).
	// Admin endpoints.
	rs.mux.HandleFunc("/admin/join", rs.handleJoin)
	rs.mux.HandleFunc("/admin/leave", rs.handleLeave)
	rs.mux.HandleFunc("/admin/stats", rs.handleStats)

	return rs
}

// handleRaftConfig routes reads locally and writes through Raft (or forwards to Leader).
func (rs *RaftServer) handleRaftConfig(w http.ResponseWriter, r *http.Request) {
	tenant, namespace, name, err := configKey(r.URL.Path)
	if err != nil || tenant == "" {
		httpError(w, http.StatusBadRequest, "invalid path: %s", r.URL.Path)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Reads go directly to local store (stale reads OK by default).
		// For consistent reads, add ?consistent=true (would call raft.VerifyLeader).
		if name == "" {
			rs.handleList(w, r, tenant, namespace)
		} else {
			rs.handleGet(w, r, tenant, namespace, name)
		}

	case http.MethodPut:
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required for PUT")
			return
		}
		rs.handleRaftPut(w, r, tenant, namespace, name)

	case http.MethodDelete:
		if name == "" {
			httpError(w, http.StatusBadRequest, "name is required for DELETE")
			return
		}
		rs.handleRaftDelete(w, r, tenant, namespace, name)

	default:
		httpError(w, http.StatusMethodNotAllowed, "method %s not allowed", r.Method)
	}
}

func (rs *RaftServer) handleRaftPut(w http.ResponseWriter, r *http.Request, tenant, namespace, name string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		httpError(w, http.StatusBadRequest, "read body: %v", err)
		return
	}

	key := storeKey(tenant, namespace, name)

	// If not leader, forward to leader.
	if !rs.node.IsLeader() {
		rs.forwardToLeader(w, r, body)
		return
	}

	op := praft.Op{Type: "put", Key: key, Value: body}
	result, err := rs.node.Apply(op, 5*time.Second)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "raft apply: %v", err)
		return
	}

	status := http.StatusOK
	if result.PrevEntry == nil {
		status = http.StatusCreated
	}

	w.Header().Set("X-Paladin-Revision", fmt.Sprintf("%d", result.Entry.Revision))
	writeJSON(w, status, &ConfigResponse{
		Revision: result.Entry.Revision,
		Configs:  []*ConfigItem{entryToConfig(result.Entry)},
	})
	log.Printf("[RAFT PUT] %s rev=%d", key, result.Entry.Revision)
}

func (rs *RaftServer) handleRaftDelete(w http.ResponseWriter, r *http.Request, tenant, namespace, name string) {
	key := storeKey(tenant, namespace, name)

	if !rs.node.IsLeader() {
		rs.forwardToLeader(w, r, nil)
		return
	}

	op := praft.Op{Type: "delete", Key: key}
	result, err := rs.node.Apply(op, 5*time.Second)
	if err != nil {
		if err.Error() == "apply error: key not found" {
			httpError(w, http.StatusNotFound, "key not found: %s", key)
			return
		}
		httpError(w, http.StatusInternalServerError, "raft apply: %v", err)
		return
	}

	writeJSON(w, http.StatusOK, &ConfigResponse{
		Revision: rs.node.Rev(),
		Configs:  []*ConfigItem{entryToConfig(result.Entry)},
	})
	log.Printf("[RAFT DELETE] %s", key)
}

// forwardToLeader proxies the request to the current Raft leader.
//
// This is the core of "ForwardRPC" — the client connects to any node,
// and if that node is a Follower, the request is transparently forwarded
// to the Leader. The client never needs to know cluster topology.
//
// Paladin's production implementation uses RPC (gorpc) for forwarding.
// We use HTTP forwarding for simplicity.
func (rs *RaftServer) forwardToLeader(w http.ResponseWriter, r *http.Request, body []byte) {
	leaderAddr := rs.node.LeaderAddr()
	if leaderAddr == "" {
		// No leader available — backoff and retry.
		// In production, this would do exponential backoff covering
		// the ~1-3s leader election window.
		httpError(w, http.StatusServiceUnavailable, "no leader available, try again later")
		return
	}

	// Build forwarded request.
	url := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.String())
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	fwdReq, err := http.NewRequestWithContext(r.Context(), r.Method, url, bodyReader)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "build forward request: %v", err)
		return
	}

	// Forward headers.
	for k, vs := range r.Header {
		for _, v := range vs {
			fwdReq.Header.Add(k, v)
		}
	}
	fwdReq.Header.Set("X-Forwarded-By", rs.node.Stats()["id"])

	resp, err := http.DefaultClient.Do(fwdReq)
	if err != nil {
		httpError(w, http.StatusBadGateway, "forward to leader %s: %v", leaderAddr, err)
		return
	}
	defer resp.Body.Close()

	// Copy leader response back to client.
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)

	log.Printf("[FORWARD] %s %s → leader %s (status %d)", r.Method, r.URL.Path, leaderAddr, resp.StatusCode)
}

// --- Admin Endpoints (Day 5/7) ---

func (rs *RaftServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	nodeID := r.URL.Query().Get("id")
	addr := r.URL.Query().Get("addr")
	if nodeID == "" || addr == "" {
		httpError(w, http.StatusBadRequest, "id and addr required")
		return
	}
	if err := rs.node.Join(nodeID, addr); err != nil {
		httpError(w, http.StatusInternalServerError, "join: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "joined": nodeID})
	log.Printf("[ADMIN] node %s (%s) joined", nodeID, addr)
}

func (rs *RaftServer) handleLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	nodeID := r.URL.Query().Get("id")
	if nodeID == "" {
		httpError(w, http.StatusBadRequest, "id required")
		return
	}
	if err := rs.node.Leave(nodeID); err != nil {
		httpError(w, http.StatusInternalServerError, "leave: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "removed": nodeID})
	log.Printf("[ADMIN] node %s left", nodeID)
}

func (rs *RaftServer) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := rs.node.Stats()
	stats["store_revision"] = fmt.Sprintf("%d", rs.node.Rev())
	stats["is_leader"] = fmt.Sprintf("%v", rs.node.IsLeader())
	writeJSON(w, http.StatusOK, stats)
}
