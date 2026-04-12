package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWatchReturnsEventsImmediately(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/config/public/prod/db_host", strings.NewReader("10.0.0.1"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/watch/public/prod/?revision=0&timeout=1", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp WatchResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(resp.Events))
	}
	if resp.Events[0].Type != "PUT" {
		t.Fatalf("expected PUT event, got %s", resp.Events[0].Type)
	}
}

func TestWatchBlocksAndReturnsOnChange(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	var resp WatchResponse
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/watch/public/prod/?revision=0&timeout=5", nil)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		json.NewDecoder(w.Body).Decode(&resp)
	}()

	time.Sleep(100 * time.Millisecond)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/config/public/prod/key1", strings.NewReader("val1"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	wg.Wait()

	if len(resp.Events) == 0 {
		t.Fatal("expected at least 1 event, got 0")
	}
}

func TestWatchTimeout(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	start := time.Now()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/watch/public/prod/?revision=0&timeout=1", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	elapsed := time.Since(start)

	var resp WatchResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if len(resp.Events) != 0 {
		t.Fatalf("expected 0 events on timeout, got %d", len(resp.Events))
	}
	if elapsed < 800*time.Millisecond {
		t.Fatalf("returned too early: %v", elapsed)
	}
}

func TestWatchPrefixFilter(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	for _, kv := range [][2]string{
		{"public/prod/key1", "v1"},
		{"public/staging/key2", "v2"},
	} {
		req := httptest.NewRequest(http.MethodPut, "/api/v1/config/"+kv[0], strings.NewReader(kv[1]))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/watch/public/prod/?revision=0&timeout=1", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var resp WatchResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Events) != 1 {
		t.Fatalf("expected 1 event for public/prod, got %d", len(resp.Events))
	}
}
