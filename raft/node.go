// Package raft provides the distributed consensus layer for PaladinCore.
//
// Day 4: We wrap the Day 1-3 single-node store behind a Raft FSM.
// All write operations go through Raft log replication before being
// applied to the local BoltDB.
//
// Architecture:
//
//	Client → Server → RaftNode.Apply(op) → Leader replicates to Followers
//	                                       → Quorum ACK → FSM.Apply(log)
//	                                       → BoltStore.Put/Delete
//	                                       → WatchCache.Append(event)
package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/smy/paladin-core/store"
)

// Op represents an operation to be replicated through Raft.
type Op struct {
	Type  string `json:"type"` // "put" or "delete"
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// OpResult is the result of applying an Op.
type OpResult struct {
	Entry     *store.Entry     `json:"entry,omitempty"`
	PrevEntry *store.Entry     `json:"prev_entry,omitempty"`
	Error     string           `json:"error,omitempty"`
}

// NodeConfig holds configuration for a Raft node.
type NodeConfig struct {
	NodeID    string // Unique node identifier
	BindAddr  string // Address for Raft transport (e.g., "127.0.0.1:9001")
	DataDir   string // Directory for Raft logs, snapshots, and BoltDB
	Bootstrap bool   // Whether to bootstrap as the initial leader
}

// Node wraps a HashiCorp Raft instance with our FSM.
type Node struct {
	config NodeConfig
	raft   *raft.Raft
	fsm    *FSM
	store  *store.WatchableStore
}

// NewNode creates and starts a new Raft node.
func NewNode(config NodeConfig) (*Node, error) {
	// Create data directory.
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// Create the underlying BoltStore + WatchableStore.
	bs, err := store.NewBoltStore(filepath.Join(config.DataDir, "data.db"))
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}
	ws := store.NewWatchableStore(bs)

	// Create the FSM.
	fsm := &FSM{store: ws}

	// Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.SnapshotThreshold = 1024
	raftConfig.SnapshotInterval = 30 * time.Second

	// Raft log store and stable store (both use BoltDB).
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("create raft log store: %w", err)
	}

	// Snapshot store.
	snapshotStore, err := raft.NewFileSnapshotStore(config.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	// Transport.
	addr, err := net.ResolveTCPAddr("tcp", config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(config.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	// Create Raft instance.
	r, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	// Bootstrap if requested.
	if config.Bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(config.NodeID),
					Address: raft.ServerAddress(config.BindAddr),
				},
			},
		}
		r.BootstrapCluster(cfg)
	}

	return &Node{
		config: config,
		raft:   r,
		fsm:    fsm,
		store:  ws,
	}, nil
}

// Apply submits an operation through Raft consensus.
// Only the Leader can apply. Returns error if not leader.
func (n *Node) Apply(op Op, timeout time.Duration) (*OpResult, error) {
	if n.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	data, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("marshal op: %w", err)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply: %w", err)
	}

	result, ok := future.Response().(*OpResult)
	if !ok {
		return nil, fmt.Errorf("unexpected response type: %T", future.Response())
	}

	if result.Error != "" {
		return nil, fmt.Errorf("apply error: %s", result.Error)
	}

	return result, nil
}

// Get reads directly from the local store (allows stale reads on Followers).
func (n *Node) Get(key string) (*store.Entry, error) {
	return n.store.Get(key)
}

// List reads directly from the local store.
func (n *Node) List(prefix string) ([]*store.Entry, error) {
	return n.store.List(prefix)
}

// Rev returns the current store revision.
func (n *Node) Rev() uint64 {
	return n.store.Rev()
}

// Store returns the underlying WatchableStore.
func (n *Node) Store() *store.WatchableStore {
	return n.store
}

// IsLeader returns true if this node is the Raft leader.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (n *Node) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// Join adds a new node to the Raft cluster. Must be called on the Leader.
func (n *Node) Join(nodeID, addr string) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	f := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}

// Leave removes a node from the Raft cluster.
func (n *Node) Leave(nodeID string) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	f := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 10*time.Second)
	return f.Error()
}

// Shutdown gracefully stops the Raft node.
func (n *Node) Shutdown() error {
	f := n.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return n.store.Close()
}

// Stats returns Raft statistics.
func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

// ErrNotLeader is returned when a write is attempted on a non-leader node.
var ErrNotLeader = fmt.Errorf("not the leader")

// --- FSM Implementation ---

// FSM implements raft.FSM for PaladinCore.
// It applies replicated log entries to the WatchableStore.
//
// CRITICAL: Apply must not block. It runs on the Raft apply goroutine.
// Any blocking here stalls the entire Raft state machine.
type FSM struct {
	store *store.WatchableStore
}

// Apply is called by Raft when a log entry is committed.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var op Op
	if err := json.Unmarshal(log.Data, &op); err != nil {
		return &OpResult{Error: fmt.Sprintf("unmarshal op: %v", err)}
	}

	switch op.Type {
	case "put":
		result, err := f.store.Put(op.Key, op.Value)
		if err != nil {
			return &OpResult{Error: err.Error()}
		}
		return &OpResult{
			Entry:     result.Entry,
			PrevEntry: result.PrevEntry,
		}
	case "delete":
		deleted, err := f.store.Delete(op.Key)
		if err != nil {
			return &OpResult{Error: err.Error()}
		}
		return &OpResult{Entry: deleted}
	default:
		return &OpResult{Error: fmt.Sprintf("unknown op type: %s", op.Type)}
	}
}

// Snapshot returns an FSM snapshot for Raft log compaction.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// Gather all entries for the snapshot.
	entries, err := f.store.List("")
	if err != nil {
		return nil, err
	}
	rev := f.store.Rev()
	return &fsmSnapshot{entries: entries, rev: rev}, nil
}

// Restore replaces the FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snap snapshotData
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	// Restore by replaying all entries.
	// Note: In production (Paladin), this does ReapDB + InstallSnapshot.
	// We simplify by re-putting all keys.
	for _, e := range snap.Entries {
		if _, err := f.store.BoltStore.Put(e.Key, e.Value); err != nil {
			return fmt.Errorf("restore key %s: %w", e.Key, err)
		}
	}
	return nil
}

// --- Snapshot ---

type snapshotData struct {
	Entries []*store.Entry `json:"entries"`
	Rev     uint64         `json:"rev"`
}

type fsmSnapshot struct {
	entries []*store.Entry
	rev     uint64
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(&snapshotData{
		Entries: s.entries,
		Rev:     s.rev,
	})
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
