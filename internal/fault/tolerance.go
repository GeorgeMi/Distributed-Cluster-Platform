package fault

import (
	"log"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/agent"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

type Tolerance struct {
	state         *cluster.State
	checkInterval time.Duration
	suspectAfter  time.Duration
	deadAfter     time.Duration
	onNodeDead    func(nodeID string, containers []*domain.Container)
	stop          chan struct{}
}

func NewTolerance(state *cluster.State, checkInterval time.Duration, suspectAfter time.Duration, deadAfter time.Duration, onNodeDead func(nodeID string, containers []*domain.Container)) *Tolerance {
	return &Tolerance{
		state:         state,
		checkInterval: checkInterval,
		suspectAfter:  suspectAfter,
		deadAfter:     deadAfter,
		onNodeDead:    onNodeDead,
		stop:          make(chan struct{}),
	}
}

func (t *Tolerance) Start() {
	ticker := time.NewTicker(t.checkInterval)
	defer ticker.Stop()

	log.Printf("fault tolerance: started, check every %s, suspect after %s, dead after %s",
		t.checkInterval, t.suspectAfter, t.deadAfter)

	for {
		select {
		case <-ticker.C:
			t.check()
		case <-t.stop:
			log.Println("fault tolerance: stopped")
			return
		}
	}
}

func (t *Tolerance) Stop() {
	close(t.stop)
}

func (t *Tolerance) check() {
	nodes := t.state.GetAllNodes()
	now := time.Now()

	for _, node := range nodes {
		if node.Status == domain.NodeDead || node.Status == domain.NodeRemoved {
			continue
		}

		since := now.Sub(node.LastHeartbeat)

		switch {
		case since >= t.deadAfter && node.Status != domain.NodeDead:
			// 30s without heartbeat -> DEAD
			log.Printf("fault tolerance: node %s marked DEAD (no heartbeat for %s)", node.ID, since.Round(time.Second))
			t.state.SetNodeStatus(node.ID, domain.NodeDead)

			// Get containers on this node and trigger recovery
			containers := t.state.GetContainersByNode(node.ID)
			if len(containers) > 0 && t.onNodeDead != nil {
				t.onNodeDead(node.ID, containers)
			}

		case since >= t.suspectAfter && node.Status == domain.NodeAlive:
			// 15s without heartbeat -> SUSPECT
			log.Printf("fault tolerance: node %s marked SUSPECT (no heartbeat for %s)", node.ID, since.Round(time.Second))
			t.state.SetNodeStatus(node.ID, domain.NodeSuspect)
		}
	}
}

// HandleLateHeartbeat handles a heartbeat from a node that was marked DEAD.
// Sends KillContainers to stop duplicate containers on the recovered node.
func (t *Tolerance) HandleLateHeartbeat(nodeID string) {
	node, exists := t.state.GetNode(nodeID)
	if !exists {
		return
	}
	if node.Status != domain.NodeDead {
		return
	}

	log.Printf("fault tolerance: late heartbeat from DEAD node %s, sending KillContainers", nodeID)

	addr := node.Address
	go func() {
		if _, err := agent.SendCommand(addr, agent.Command{Type: agent.CmdKillContainers}); err != nil {
			log.Printf("fault tolerance: failed to send KillContainers to %s: %v", nodeID, err)
		}
	}()

	// Mark containers on this node as stopped (they were already rescheduled)
	containers := t.state.GetContainersByNode(nodeID)
	for _, c := range containers {
		t.state.SetContainerStatus(c.ID, domain.ContainerStopped)
	}

	// Node goes back to ALIVE, available for new scheduling
	t.state.SetNodeStatus(nodeID, domain.NodeAlive)
	log.Printf("fault tolerance: node %s back to ALIVE (containers killed, available for scheduling)", nodeID)
}
