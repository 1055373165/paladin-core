// PaladinCore — Final entry point supporting both standalone and Raft cluster modes.
//
// Usage:
//
//	paladin-core serve [--addr :8080]                          Standalone mode (Day 1-3)
//	paladin-core cluster --id node1 --raft 127.0.0.1:9001      Raft cluster mode (Day 4-7)
//	              --http :8080 [--join leader:8080] [--bootstrap]
//	paladin-core put/get/delete/list/rev                        Local CLI (Day 1)
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	praft "github.com/smy/paladin-core/raft"
	"github.com/smy/paladin-core/server"
	"github.com/smy/paladin-core/store"
)

const defaultDBPath = "paladin-core.db"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "serve":
		runStandalone()
	case "cluster":
		runCluster()
	case "put", "get", "delete", "list", "rev":
		runCLI(cmd)
	default:
		usage()
		os.Exit(1)
	}
}

func runStandalone() {
	addr := ":8080"
	if len(os.Args) >= 3 {
		addr = os.Args[2]
	}
	bs, err := store.NewBoltStore(defaultDBPath)
	if err != nil {
		fatal("open store: %v", err)
	}
	ws := store.NewWatchableStore(bs)
	defer ws.Close()

	srv := server.New(ws)
	log.Printf("PaladinCore [standalone] on %s", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		fatal("listen: %v", err)
	}
}

func runCluster() {
	fs := flag.NewFlagSet("cluster", flag.ExitOnError)
	nodeID := fs.String("id", "", "Node ID (required)")
	raftAddr := fs.String("raft", "127.0.0.1:9001", "Raft bind address")
	httpAddr := fs.String("http", ":8080", "HTTP listen address")
	dataDir := fs.String("data", "", "Data directory (default: data-{id})")
	bootstrap := fs.Bool("bootstrap", false, "Bootstrap as initial leader")
	join := fs.String("join", "", "Leader HTTP address to join (e.g., localhost:8080)")
	fs.Parse(os.Args[2:])

	if *nodeID == "" {
		fatal("--id is required")
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data-%s", *nodeID)
	}

	node, err := praft.NewNode(praft.NodeConfig{
		NodeID:    *nodeID,
		BindAddr:  *raftAddr,
		DataDir:   *dataDir,
		Bootstrap: *bootstrap,
	})
	if err != nil {
		fatal("create raft node: %v", err)
	}
	defer node.Shutdown()

	srv := server.NewRaftServer(node)

	// If --join specified, request to join the cluster.
	if *join != "" {
		url := fmt.Sprintf("http://%s/admin/join?id=%s&addr=%s", *join, *nodeID, *raftAddr)
		resp, err := http.Post(url, "", nil)
		if err != nil {
			fatal("join cluster: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			fatal("join cluster: status %d", resp.StatusCode)
		}
		log.Printf("Joined cluster via %s", *join)
	}

	log.Printf("PaladinCore [raft] node=%s raft=%s http=%s bootstrap=%v",
		*nodeID, *raftAddr, *httpAddr, *bootstrap)
	if err := http.ListenAndServe(*httpAddr, srv); err != nil {
		fatal("listen: %v", err)
	}
}

func runCLI(cmd string) {
	s, err := store.NewBoltStore(defaultDBPath)
	if err != nil {
		fatal("open store: %v", err)
	}
	defer s.Close()

	switch cmd {
	case "put":
		if len(os.Args) < 4 {
			fatal("usage: paladin-core put <key> <value>")
		}
		res, err := s.Put(os.Args[2], []byte(os.Args[3]))
		if err != nil {
			fatal("put: %v", err)
		}
		fmt.Printf("OK  rev=%d  version=%d  key=%s\n", res.Entry.Revision, res.Entry.Version, os.Args[2])
	case "get":
		if len(os.Args) < 3 {
			fatal("usage: paladin-core get <key>")
		}
		e, err := s.Get(os.Args[2])
		if err != nil {
			fatal("get: %v", err)
		}
		fmt.Printf("key=%s  value=%s  rev=%d\n", e.Key, e.Value, e.Revision)
	case "delete":
		if len(os.Args) < 3 {
			fatal("usage: paladin-core delete <key>")
		}
		d, err := s.Delete(os.Args[2])
		if err != nil {
			fatal("delete: %v", err)
		}
		fmt.Printf("DELETED  key=%s  rev=%d\n", d.Key, d.Revision)
	case "list":
		prefix := ""
		if len(os.Args) >= 3 {
			prefix = os.Args[2]
		}
		entries, _ := s.List(prefix)
		for _, e := range entries {
			fmt.Printf("  %-30s = %-20s  rev=%d\n", e.Key, e.Value, e.Revision)
		}
	case "rev":
		fmt.Printf("revision: %d\n", s.Rev())
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `PaladinCore — Distributed Configuration Center

Commands:
  serve [addr]                  Standalone HTTP server
  cluster --id ID [options]     Raft cluster node

Cluster Options:
  --id ID              Node ID (required)
  --raft ADDR          Raft bind address (default 127.0.0.1:9001)
  --http ADDR          HTTP listen address (default :8080)
  --data DIR           Data directory (default data-{id})
  --bootstrap          Bootstrap as initial leader
  --join LEADER:PORT   Join an existing cluster

CLI Commands:
  put <key> <value>    Create/update config
  get <key>            Get config
  delete <key>         Delete config
  list [prefix]        List configs
  rev                  Show revision`)
}

func fatal(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+f+"\n", a...)
	os.Exit(1)
}
