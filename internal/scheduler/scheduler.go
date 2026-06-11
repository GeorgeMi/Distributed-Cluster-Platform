package scheduler

import (
	"fmt"
	"log"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/loadbalancer"
)

type Scheduler struct {
	state *cluster.State
	lb    loadbalancer.LoadBalancer
}

func NewScheduler(state *cluster.State, lb loadbalancer.LoadBalancer) *Scheduler {
	return &Scheduler{
		state: state,
		lb:    lb,
	}
}

// Schedule finds the best node for a service and returns it.
// It filters nodes in the service's pool that are alive and have enough resources.
func (s *Scheduler) Schedule(service *domain.Service) (*domain.Node, error) {
	poolNodes := s.state.GetPoolNodes(service.PoolID)
	if len(poolNodes) == 0 {
		return nil, fmt.Errorf("no nodes in pool %s", service.PoolID)
	}

	// Filter: alive + enough resources
	var candidates []*domain.Node
	for _, n := range poolNodes {
		if n.IsAlive() && n.AvailableCPU() >= service.RequiredCPU && n.AvailableRAM() >= service.RequiredRAM {
			c := *n
			c.Containers = s.state.GetActiveContainersByNode(n.ID)
			candidates = append(candidates, &c)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no nodes with enough resources (need %.1f CPU, %d MB RAM)", service.RequiredCPU, service.RequiredRAM)
	}

	node := s.lb.SelectNode(candidates, service)
	if node == nil {
		return nil, fmt.Errorf("load balancer returned no node")
	}

	log.Printf("scheduler: selected node %s for service %s (%.1f CPU, %d MB RAM available)",
		node.ID, service.Name, node.AvailableCPU(), node.AvailableRAM())

	return node, nil
}
