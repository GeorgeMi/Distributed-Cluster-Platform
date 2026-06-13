package scheduler

import (
	"testing"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/loadbalancer"
)

func setupState() *cluster.State {
	state := cluster.NewState()
	state.AddPool(&domain.ResourcePool{ID: "pool-1", Name: "pool-1", MinCPU: 0, MaxCPU: 128, MinRAM: 0, MaxRAM: 65536})

	node := &domain.Node{ID: "n1", Status: domain.NodeAlive, TotalCPU: 8, UsedCPU: 2, TotalRAM: 16384, UsedRAM: 4096}
	state.AddNode(node)
	state.AssignNodeToPool("n1", node)

	return state
}

func TestSchedule_FindsNode(t *testing.T) {
	state := setupState()
	sched := NewScheduler(state, &loadbalancer.LeastConnectionsLB{})

	svc := &domain.Service{PoolID: "pool-1", RequiredCPU: 1, RequiredRAM: 1024}
	node, err := sched.Schedule(svc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.ID != "n1" {
		t.Errorf("expected n1, got %s", node.ID)
	}
}

func TestSchedule_NotEnoughResources(t *testing.T) {
	state := setupState()
	sched := NewScheduler(state, &loadbalancer.LeastConnectionsLB{})

	svc := &domain.Service{PoolID: "pool-1", RequiredCPU: 100, RequiredRAM: 1024}
	_, err := sched.Schedule(svc)
	if err == nil {
		t.Error("expected error for insufficient resources")
	}
}

func TestPlan_AllOrNothing(t *testing.T) {
	state := setupState()
	sched := NewScheduler(state, &loadbalancer.LeastConnectionsLB{})

	// The node has 6 CPU available: one replica of 4 CPU fits, two do not.
	svc := &domain.Service{PoolID: "pool-1", RequiredCPU: 4, RequiredRAM: 1024}
	if _, err := sched.Plan(svc, 1); err != nil {
		t.Fatalf("one replica should fit: %v", err)
	}
	if _, err := sched.Plan(svc, 2); err == nil {
		t.Error("expected error: two replicas need 8 CPU but only 6 are available")
	}
}

func TestSchedule_EmptyPool(t *testing.T) {
	state := cluster.NewState()
	state.AddPool(&domain.ResourcePool{ID: "empty", Name: "empty", MinCPU: 0, MaxCPU: 128, MinRAM: 0, MaxRAM: 65536})
	sched := NewScheduler(state, &loadbalancer.LeastConnectionsLB{})

	svc := &domain.Service{PoolID: "empty", RequiredCPU: 1, RequiredRAM: 1024}
	_, err := sched.Schedule(svc)
	if err == nil {
		t.Error("expected error for empty pool")
	}
}
