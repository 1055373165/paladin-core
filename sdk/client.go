// Package sdk provides a Go client for PaladinCore.
//
// Day 6: Three-phase lifecycle from Paladin SDK V2:
//   1. Startup: full pull (or fallback to local cache)
//   2. Runtime: long-poll for incremental updates
//   3. Shutdown: graceful stop
package sdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Config holds SDK configuration.
type Config struct {
	Addrs        []string
	Tenant       string
	Namespace    string
	CacheDir     string        // Local cache for fallback
	PollTimeout  time.Duration // Default 30s
	RetryBackoff time.Duration // Default 1s
}

// Client is the PaladinCore SDK client.
type Client struct {
	config   Config
	mu       sync.RWMutex
	configs  map[string][]byte
	revision uint64
	watchers map[string][]func(string, []byte, []byte)
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	client   *http.Client
}

type configResponse struct {
	Revision uint64       `json:"revision"`
	Configs  []configItem `json:"configs"`
}
type configItem struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Revision uint64 `json:"revision"`
}
type watchResponse struct {
	Revision uint64       `json:"revision"`
	Events   []watchEvent `json:"events"`
}
type watchEvent struct {
	Type      string      `json:"type"`
	Entry     *configItem `json:"entry"`
	PrevEntry *configItem `json:"prev_entry,omitempty"`
}

// New creates a client, does full pull, starts watch loop.
func New(cfg Config) (*Client, error) {
	if cfg.PollTimeout == 0 {
		cfg.PollTimeout = 30 * time.Second
	}
	if cfg.RetryBackoff == 0 {
		cfg.RetryBackoff = 1 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		config: cfg, configs: make(map[string][]byte),
		watchers: make(map[string][]func(string, []byte, []byte)),
		ctx: ctx, cancel: cancel,
		client: &http.Client{Timeout: cfg.PollTimeout + 5*time.Second},
	}
	if err := c.fullPull(); err != nil {
		log.Printf("[SDK] full pull failed: %v, trying cache", err)
		if cacheErr := c.loadFromCache(); cacheErr != nil {
			log.Printf("[SDK] cache load failed: %v", cacheErr)
		}
	}
	c.wg.Add(1)
	go c.watchLoop()
	return c, nil
}

func (c *Client) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.configs[key]
	return v, ok
}

func (c *Client) GetAll() map[string][]byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m := make(map[string][]byte, len(c.configs))
	for k, v := range c.configs {
		m[k] = v
	}
	return m
}

// OnChange registers a callback. key="" watches all keys.
func (c *Client) OnChange(key string, fn func(string, []byte, []byte)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.watchers[key] = append(c.watchers[key], fn)
}

func (c *Client) Close() { c.cancel(); c.wg.Wait() }

func (c *Client) fullPull() error {
	url := fmt.Sprintf("http://%s/api/v1/config/%s/%s/",
		c.pickAddr(), c.config.Tenant, c.config.Namespace)
	resp, err := c.client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var cr configResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return err
	}
	c.mu.Lock()
	for _, item := range cr.Configs {
		c.configs[item.Key] = []byte(item.Value)
	}
	c.revision = cr.Revision
	c.mu.Unlock()
	c.saveToCache()
	log.Printf("[SDK] full pull: %d configs, rev=%d", len(cr.Configs), cr.Revision)
	return nil
}

func (c *Client) watchLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		c.mu.RLock()
		rev := c.revision
		c.mu.RUnlock()
		url := fmt.Sprintf("http://%s/api/v1/watch/%s/%s/?revision=%d&timeout=%d",
			c.pickAddr(), c.config.Tenant, c.config.Namespace,
			rev, int(c.config.PollTimeout.Seconds()))
		resp, err := c.client.Get(url)
		if err != nil {
			log.Printf("[SDK] watch error: %v, retry in %v", err, c.config.RetryBackoff)
			c.sleep(c.config.RetryBackoff)
			continue
		}
		var wr watchResponse
		json.NewDecoder(resp.Body).Decode(&wr)
		resp.Body.Close()
		if len(wr.Events) > 0 {
			c.applyEvents(wr.Events)
			c.saveToCache()
		}
		if wr.Revision > 0 {
			c.mu.Lock()
			c.revision = wr.Revision
			c.mu.Unlock()
		}
	}
}

func (c *Client) applyEvents(events []watchEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range events {
		key := e.Entry.Key
		newVal := []byte(e.Entry.Value)
		oldVal := c.configs[key]
		switch e.Type {
		case "PUT":
			c.configs[key] = newVal
		case "DELETE":
			delete(c.configs, key)
			newVal = nil
		}
		for _, fn := range c.watchers[key] {
			fn(key, oldVal, newVal)
		}
		for _, fn := range c.watchers[""] {
			fn(key, oldVal, newVal)
		}
		log.Printf("[SDK] %s %s", e.Type, key)
	}
}

func (c *Client) pickAddr() string { return c.config.Addrs[0] }

func (c *Client) sleep(d time.Duration) {
	select {
	case <-time.After(d):
	case <-c.ctx.Done():
	}
}

// --- Local Cache with SHA-256 Checksum ---

type cacheFile struct {
	Checksum string            `json:"checksum"`
	Revision uint64            `json:"revision"`
	Configs  map[string]string `json:"configs"`
}

func (c *Client) cachePath() string {
	if c.config.CacheDir == "" {
		return ""
	}
	return filepath.Join(c.config.CacheDir,
		fmt.Sprintf("paladin_%s_%s.json", c.config.Tenant, c.config.Namespace))
}

func (c *Client) saveToCache() {
	path := c.cachePath()
	if path == "" {
		return
	}
	os.MkdirAll(filepath.Dir(path), 0755)
	c.mu.RLock()
	cfgs := make(map[string]string, len(c.configs))
	for k, v := range c.configs {
		cfgs[k] = string(v)
	}
	rev := c.revision
	c.mu.RUnlock()
	data, _ := json.Marshal(cfgs)
	cf := cacheFile{Checksum: sha256Sum(data), Revision: rev, Configs: cfgs}
	out, _ := json.MarshalIndent(cf, "", "  ")
	os.WriteFile(path, out, 0644)
}

func (c *Client) loadFromCache() error {
	path := c.cachePath()
	if path == "" {
		return fmt.Errorf("no cache dir")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cf cacheFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return fmt.Errorf("corrupt cache: %w", err)
	}
	cfgData, _ := json.Marshal(cf.Configs)
	if sha256Sum(cfgData) != cf.Checksum {
		return fmt.Errorf("cache checksum mismatch")
	}
	c.mu.Lock()
	for k, v := range cf.Configs {
		c.configs[k] = []byte(v)
	}
	c.revision = cf.Revision
	c.mu.Unlock()
	log.Printf("[SDK] loaded %d configs from cache (rev=%d)", len(cf.Configs), cf.Revision)
	return nil
}

func sha256Sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
