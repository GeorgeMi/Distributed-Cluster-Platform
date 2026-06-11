package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
	"github.com/hashicorp/raft"
)

// LogEntry is a command that modifies the cluster state via Raft.
type LogEntry struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// Log entry types
const (
	LogAddNode            = "AddNode"
	LogRemoveNode         = "RemoveNode"
	LogSetNodeStatus      = "SetNodeStatus"
	LogAddService         = "AddService"
	LogRemoveService      = "RemoveService"
	LogSetServiceReplicas = "SetServiceReplicas"
	LogAddContainer       = "AddContainer"
	LogRemoveContainer    = "RemoveContainer"
	LogSetContainerStatus = "SetContainerStatus"
)

// FSM implements raft.FSM for the cluster state.
type FSM struct {
	state *State
}

func NewFSM(state *State) *FSM {
	return &FSM{state: state}
}

// Apply is called by Raft when a log entry is committed.
func (f *FSM) Apply(l *raft.Log) any {
	var entry LogEntry
	if err := json.Unmarshal(l.Data, &entry); err != nil {
		log.Printf("fsm: unmarshal error: %v", err)
		return err
	}

	switch entry.Type {
	case LogAddNode:
		var node domain.Node
		json.Unmarshal(entry.Data, &node)
		f.state.AddNode(&node)
		f.state.AssignNodeToPool(node.ID, &node)
		log.Printf("fsm: apply AddNode %s", node.ID)

	case LogRemoveNode:
		var args struct {
			ID string `json:"id"`
		}
		json.Unmarshal(entry.Data, &args)
		f.state.RemoveNode(args.ID)

	case LogSetNodeStatus:
		var args struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		json.Unmarshal(entry.Data, &args)
		f.state.SetNodeStatus(args.ID, args.Status)

	case LogAddService:
		var svc domain.Service
		json.Unmarshal(entry.Data, &svc)
		f.state.AddService(&svc)
		log.Printf("fsm: apply AddService %s", svc.Name)

	case LogRemoveService:
		var args struct {
			ID string `json:"id"`
		}
		json.Unmarshal(entry.Data, &args)
		f.state.RemoveService(args.ID)

	case LogSetServiceReplicas:
		var args struct {
			ID       string `json:"id"`
			Replicas int    `json:"replicas"`
		}
		json.Unmarshal(entry.Data, &args)
		f.state.SetServiceReplicas(args.ID, args.Replicas)

	case LogAddContainer:
		var c domain.Container
		json.Unmarshal(entry.Data, &c)
		f.state.AddContainer(&c)

	case LogRemoveContainer:
		var args struct {
			ID string `json:"id"`
		}
		json.Unmarshal(entry.Data, &args)
		f.state.RemoveContainer(args.ID)

	case LogSetContainerStatus:
		var args struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		json.Unmarshal(entry.Data, &args)
		f.state.SetContainerStatus(args.ID, args.Status)

	default:
		log.Printf("fsm: unknown entry type: %s", entry.Type)
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{data: f.state.Snapshot()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var snap Snapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return fmt.Errorf("snapshot decode: %w", err)
	}
	f.state.Restore(snap)
	log.Printf("fsm: restored state (%d nodes, %d services, %d containers)",
		len(snap.Nodes), len(snap.Services), len(snap.Containers))
	return nil
}

type fsmSnapshot struct {
	data Snapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	data, err := json.Marshal(s.data)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("snapshot marshal: %w", err)
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return fmt.Errorf("snapshot write: %w", err)
	}
	return nil
}

func (s *fsmSnapshot) Release() {}
