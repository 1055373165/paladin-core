package sdk

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/smy/paladin-core/server"
	"github.com/smy/paladin-core/store"
)

func testSDKServer(t *testing.T) (*httptest.Server, *store.WatchableStore) {
	t.Helper()
	bs, _ := store.NewBoltStore(t.TempDir() + "/test.db")
	ws := store.NewWatchableStore(bs)
	srv := server.New(ws)
	ts := httptest.NewServer(srv)
	t.Cleanup(func() { ts.Close(); ws.Close() })
	return ts, ws
}

func TestSDKFullPullAndGet(t *testing.T) {
	ts, ws := testSDKServer(t)

	// Seed some data.
	ws.Put("public/prod/db_host", []byte("10.0.0.1"))
	ws.Put("public/prod/db_port", []byte("3306"))

	addr := strings.TrimPrefix(ts.URL, "http://")
	c, err := New(Config{
		Addrs:     []string{addr},
		Tenant:    "public",
		Namespace: "prod",
		CacheDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	v, ok := c.Get("public/prod/db_host")
	if !ok || string(v) != "10.0.0.1" {
		t.Fatalf("expected 10.0.0.1, got %s (ok=%v)", v, ok)
	}
}

func TestSDKWatchUpdates(t *testing.T) {
	ts, ws := testSDKServer(t)
	addr := strings.TrimPrefix(ts.URL, "http://")

	c, _ := New(Config{
		Addrs:        []string{addr},
		Tenant:       "public",
		Namespace:    "prod",
		PollTimeout:  2 * time.Second,
		RetryBackoff: 100 * time.Millisecond,
	})
	defer c.Close()

	// Register a change callback.
	changed := make(chan string, 1)
	c.OnChange("public/prod/db_host", func(key string, old, new []byte) {
		changed <- string(new)
	})

	// Put data — SDK should detect via long poll.
	ws.Put("public/prod/db_host", []byte("10.0.0.2"))

	select {
	case val := <-changed:
		if val != "10.0.0.2" {
			t.Fatalf("expected 10.0.0.2, got %s", val)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch event")
	}
}

func TestSDKCacheFallback(t *testing.T) {
	cacheDir := t.TempDir()

	// Phase 1: Connect to real server, populate cache.
	ts, ws := testSDKServer(t)
	ws.Put("public/prod/key1", []byte("cached_value"))
	addr := strings.TrimPrefix(ts.URL, "http://")

	c1, _ := New(Config{
		Addrs:     []string{addr},
		Tenant:    "public",
		Namespace: "prod",
		CacheDir:  cacheDir,
	})
	c1.Close()
	ts.Close()

	// Phase 2: Server is down — SDK should fall back to cache.
	c2, _ := New(Config{
		Addrs:        []string{"127.0.0.1:1"}, // unreachable
		Tenant:       "public",
		Namespace:    "prod",
		CacheDir:     cacheDir,
		PollTimeout:  1 * time.Second,
		RetryBackoff: 100 * time.Millisecond,
	})
	defer c2.Close()

	v, ok := c2.Get("public/prod/key1")
	if !ok || string(v) != "cached_value" {
		t.Fatalf("expected cached_value from fallback, got %s (ok=%v)", v, ok)
	}
}

func TestSDKCacheChecksumValidation(t *testing.T) {
	cacheDir := t.TempDir()

	// Write a corrupted cache file.
	path := cacheDir + "/paladin_public_prod.json"
	os.WriteFile(path, []byte(`{"checksum":"bad","revision":1,"configs":{"k":"v"}}`), 0644)

	c, _ := New(Config{
		Addrs:        []string{"127.0.0.1:1"},
		Tenant:       "public",
		Namespace:    "prod",
		CacheDir:     cacheDir,
		PollTimeout:  1 * time.Second,
		RetryBackoff: 100 * time.Millisecond,
	})
	defer c.Close()

	// With corrupted cache, should have no configs.
	_, ok := c.Get("k")
	if ok {
		t.Fatal("expected no config from corrupted cache")
	}
}

func TestSDKServerDown(t *testing.T) {
	// Server unreachable, no cache → empty configs, but no panic.
	c, err := New(Config{
		Addrs:        []string{"127.0.0.1:1"},
		Tenant:       "public",
		Namespace:    "prod",
		PollTimeout:  1 * time.Second,
		RetryBackoff: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if len(c.GetAll()) != 0 {
		t.Fatal("expected empty configs when server down and no cache")
	}

	// Ensure health endpoint doesn't exist on the fake addr.
	resp, err := http.Get("http://127.0.0.1:1/healthz")
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected connection refused")
	}
}
