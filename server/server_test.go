package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/smy/paladin-core/store"
)

func testServer(t *testing.T) (*Server, func()) {
	t.Helper()
	dir := t.TempDir()
	bs, err := store.NewBoltStore(dir + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	ws := store.NewWatchableStore(bs)
	srv := New(ws)
	return srv, func() { ws.Close() }
}


func TestPutAndGetConfig(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	// PUT a new config.
	req := httptest.NewRequest(http.MethodPut, "/api/v1/config/public/prod/db_host", strings.NewReader("10.0.0.1"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// Check X-Paladin-Revision header.
	rev := w.Header().Get("X-Paladin-Revision")
	if rev != "1" {
		t.Fatalf("expected revision 1, got %s", rev)
	}

	// GET the config.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/config/public/prod/db_host", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp ConfigResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Configs) != 1 || resp.Configs[0].Value != "10.0.0.1" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestUpdateReturns200(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	// First PUT → 201.
	req := httptest.NewRequest(http.MethodPut, "/api/v1/config/public/prod/key", strings.NewReader("v1"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	// Second PUT → 200 (update).
	req = httptest.NewRequest(http.MethodPut, "/api/v1/config/public/prod/key", strings.NewReader("v2"))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestGetNotFound(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/config/public/prod/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestDeleteConfig(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	// Create then delete.
	req := httptest.NewRequest(http.MethodPut, "/api/v1/config/public/prod/temp", strings.NewReader("val"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	req = httptest.NewRequest(http.MethodDelete, "/api/v1/config/public/prod/temp", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Verify it's gone.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/config/public/prod/temp", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListByNamespace(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	// Add configs in different namespaces.
	for _, kv := range [][2]string{
		{"public/prod/db_host", "10.0.0.1"},
		{"public/prod/db_port", "3306"},
		{"public/staging/db_host", "10.0.1.1"},
	} {
		req := httptest.NewRequest(http.MethodPut, "/api/v1/config/"+kv[0], strings.NewReader(kv[1]))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
	}

	// List public/prod/ — should get 2.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/config/public/prod/", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var resp ConfigResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Count != 2 {
		t.Fatalf("expected 2 configs in public/prod, got %d", resp.Count)
	}

	// List public/ — should get 3 (all namespaces).
	req = httptest.NewRequest(http.MethodGet, "/api/v1/config/public/", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Count != 3 {
		t.Fatalf("expected 3 configs in public/, got %d", resp.Count)
	}
}

func TestHealthz(t *testing.T) {
	srv, cleanup := testServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("expected 200 ok, got %d %s", w.Code, w.Body.String())
	}
}
