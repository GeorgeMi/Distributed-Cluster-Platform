package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// RaftNode wraps a Raft instance for the cluster.
type RaftNode struct {
	raft *raft.Raft
	fsm  *FSM
}

// RaftPeer represents another master node in the Raft cluster.
type RaftPeer struct {
	ID      string
	Address string
}

// RaftConfig holds configuration for setting up Raft.
type RaftConfig struct {
	NodeID    string
	BindAddr  string
	DataDir   string
	Bootstrap bool
	Peers     []RaftPeer
}

// NewRaftNode creates and starts a Raft node.
func NewRaftNode(cfg RaftConfig, state *State) (*RaftNode, error) {
	fsm := NewFSM(state)

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotInterval = 30 * time.Second
	raftCfg.SnapshotThreshold = 100

	// Create data directory
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// Log store and stable store (BoltDB)
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}

	// Snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	// Bootstrap cluster if this is the first node
	if cfg.Bootstrap {
		servers := []raft.Server{
			{ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(cfg.BindAddr)},
		}
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.ID),
				Address: raft.ServerAddress(peer.Address),
			})
		}
		future := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := future.Error(); err != nil {
			log.Printf("raft: bootstrap (may already be bootstrapped): %v", err)
		}
	}

	log.Printf("raft: node %s started on %s (bootstrap=%v)", cfg.NodeID, cfg.BindAddr, cfg.Bootstrap)

	return &RaftNode{raft: r, fsm: fsm}, nil
}

// Apply submits a log entry through Raft. Only the leader can apply.
func (rn *RaftNode) Apply(entryType string, data any) error {
	if rn.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	entry := LogEntry{
		Type: entryType,
		Data: jsonData,
	}
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}

	future := rn.raft.Apply(entryBytes, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
	}
	return nil
}

// IsLeader returns true if this node is the Raft leader.
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// LeaderAddress returns the address of the current leader.
func (rn *RaftNode) LeaderAddress() string {
	addr, _ := rn.raft.LeaderWithID()
	return string(addr)
}
