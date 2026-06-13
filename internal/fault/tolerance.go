package fault

import (
	"log"
	"sync"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/agent"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

type Tolerance struct {
	state                *cluster.State
	checkInterval        time.Duration
	suspectAfter         time.Duration
	deadAfter            time.Duration
	isLeader             func() bool
	applyContainerStatus func(containerID, status string)
	onNodeDead           func(nodeID string, containers []*domain.Container)
	mu                   sync.Mutex
	recovering           map[string]bool
	stop                 chan struct{}
}

func NewTolerance(state *cluster.State, checkInterval, suspectAfter, deadAfter time.Duration,
	isLeader func() bool,
	applyContainerStatus func(containerID, status string),
	onNodeDead func(nodeID string, containers []*domain.Container),
) *Tolerance {
	return &Tolerance{
		state:                state,
		checkInterval:        checkInterval,
		suspectAfter:         suspectAfter,
		deadAfter:            deadAfter,
		isLeader:             isLeader,
		applyContainerStatus: applyContainerStatus,
		onNodeDead:           onNodeDead,
		recovering:           make(map[string]bool),
		stop:                 make(chan struct{}),
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
			log.Printf("fault tolerance: node %s marked DEAD (no heartbeat for %s)", node.ID, since.Round(time.Second))
			t.state.SetNodeStatus(node.ID, domain.NodeDead)

			containers := t.activeContainersOnNode(node.ID)
			if len(containers) > 0 && t.onNodeDead != nil {
				t.onNodeDead(node.ID, containers)
			}

		case since >= t.suspectAfter && node.Status == domain.NodeAlive:
			log.Printf("fault tolerance: node %s marked SUSPECT (no heartbeat for %s)", node.ID, since.Round(time.Second))
			t.state.SetNodeStatus(node.ID, domain.NodeSuspect)
		}
	}
}

// activeContainersOnNode returns only containers that are (or are about to be)
// running on the node. Stopped or failed containers must not be recovered.
func (t *Tolerance) activeContainersOnNode(nodeID string) []*domain.Container {
	var active []*domain.Container
	for _, c := range t.state.GetContainersByNode(nodeID) {
		switch c.Status {
		case domain.ContainerRunning, domain.ContainerScheduled, domain.ContainerRescheduling:
			active = append(active, c)
		}
	}
	return active
}

// HandleLateHeartbeat handles a heartbeat from a node marked DEAD. Only the
// leader kills the duplicate containers and brings the node back to ALIVE,
// and only after the kill succeeds. Followers just mark the node ALIVE.
func (t *Tolerance) HandleLateHeartbeat(nodeID string) {
	node, exists := t.state.GetNode(nodeID)
	if !exists || node.Status != domain.NodeDead {
		return
	}

	if t.isLeader != nil && !t.isLeader() {
		t.state.SetNodeStatus(nodeID, domain.NodeAlive)
		return
	}

	t.mu.Lock()
	if t.recovering[nodeID] {
		t.mu.Unlock()
		return
	}
	t.recovering[nodeID] = true
	t.mu.Unlock()

	log.Printf("fault tolerance: late heartbeat from DEAD node %s, sending KillContainers", nodeID)

	addr := node.Address
	go func() {
		defer func() {
			t.mu.Lock()
			delete(t.recovering, nodeID)
			t.mu.Unlock()
		}()

		if _, err := agent.SendCommand(addr, agent.Command{Type: agent.CmdKillContainers}); err != nil {
			log.Printf("fault tolerance: failed to send KillContainers to %s: %v", nodeID, err)
			return
		}

		for _, c := range t.activeContainersOnNode(nodeID) {
			t.setContainerStatus(c.ID, domain.ContainerStopped)
		}

		t.state.SetNodeStatus(nodeID, domain.NodeAlive)
		log.Printf("fault tolerance: node %s back to ALIVE (containers killed, available for scheduling)", nodeID)
	}()
}

func (t *Tolerance) setContainerStatus(containerID, status string) {
	if t.applyContainerStatus != nil {
		t.applyContainerStatus(containerID, status)
		return
	}
	t.state.SetContainerStatus(containerID, status)
}
