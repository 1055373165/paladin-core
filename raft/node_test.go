package raft

import (
	"testing"
	"time"
)

func singleNode(t *testing.T) *Node {
	t.Helper()
	dir := t.TempDir()
	n, err := NewNode(NodeConfig{
		NodeID:    "node1",
		BindAddr:  "127.0.0.1:0", // random port
		DataDir:   dir,
		Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Shutdown() })

	// Wait for leader election.
	deadline := time.Now().Add(5 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for leader election")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return n
}

func TestRaftPutAndGet(t *testing.T) {
	n := singleNode(t)

	// Put through Raft.
	result, err := n.Apply(Op{Type: "put", Key: "public/prod/db_host", Value: []byte("10.0.0.1")}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.Entry.Version != 1 {
		t.Fatalf("expected version 1, got %d", result.Entry.Version)
	}

	// Get directly from local store.
	entry, err := n.Get("public/prod/db_host")
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Value) != "10.0.0.1" {
		t.Fatalf("expected 10.0.0.1, got %s", entry.Value)
	}
}

func TestRaftDelete(t *testing.T) {
	n := singleNode(t)

	n.Apply(Op{Type: "put", Key: "temp", Value: []byte("val")}, 5*time.Second)

	result, err := n.Apply(Op{Type: "delete", Key: "temp"}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Entry.Value) != "val" {
		t.Fatalf("expected deleted value 'val', got '%s'", result.Entry.Value)
	}

	_, err = n.Get("temp")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestRaftRevisionIncrement(t *testing.T) {
	n := singleNode(t)

	for i := 0; i < 5; i++ {
		n.Apply(Op{Type: "put", Key: "counter", Value: []byte("v")}, 5*time.Second)
	}

	if n.Rev() != 5 {
		t.Fatalf("expected rev 5, got %d", n.Rev())
	}
}

func TestRaftWatchIntegration(t *testing.T) {
	n := singleNode(t)

	// Put through Raft → should emit watch event.
	n.Apply(Op{Type: "put", Key: "public/prod/key1", Value: []byte("v1")}, 5*time.Second)

	events := n.Store().WatchCache().WaitForEvents(0, "public/prod/", 1*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 watch event, got %d", len(events))
	}
	if string(events[0].Entry.Value) != "v1" {
		t.Fatalf("expected watch event value 'v1', got '%s'", events[0].Entry.Value)
	}
}

func TestNotLeaderError(t *testing.T) {
	// Create a non-bootstrapped node — it will never become leader.
	dir := t.TempDir()
	n, err := NewNode(NodeConfig{
		NodeID:    "follower",
		BindAddr:  "127.0.0.1:0",
		DataDir:   dir,
		Bootstrap: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()

	_, err = n.Apply(Op{Type: "put", Key: "k", Value: []byte("v")}, 1*time.Second)
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}
