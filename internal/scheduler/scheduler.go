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

// Plan finds a node for every one of the n replicas, or fails without
// assigning anything (all-or-nothing). The resources and ports consumed by
// each replica are subtracted from the chosen node's free capacity before
// the next replica is placed, so a single request can never oversubscribe
// a node.
func (s *Scheduler) Plan(service *domain.Service, n int) ([]*domain.Node, error) {
	poolNodes := s.state.GetPoolNodes(service.PoolID)
	if len(poolNodes) == 0 {
		return nil, fmt.Errorf("no nodes in pool %s", service.PoolID)
	}

	// Working copies of the alive nodes; the plan mutates these copies,
	// never the real cluster state.
	var working []*domain.Node
	usedPorts := make(map[string]map[int]bool)
	for _, pn := range poolNodes {
		if !pn.IsAlive() {
			continue
		}
		c := *pn
		c.Containers = s.state.GetActiveContainersByNode(pn.ID)
		working = append(working, &c)
		ports := make(map[int]bool)
		for _, ctr := range c.Containers {
			if svc, ok := s.state.GetService(ctr.ServiceID); ok {
				for _, p := range svc.Ports {
					ports[p] = true
				}
			}
		}
		usedPorts[c.ID] = ports
	}

	plan := make([]*domain.Node, 0, n)
	for i := 0; i < n; i++ {
		var candidates []*domain.Node
		for _, c := range working {
			if c.AvailableCPU() >= service.RequiredCPU && c.AvailableRAM() >= service.RequiredRAM &&
				!portConflict(usedPorts[c.ID], service.Ports) {
				candidates = append(candidates, c)
			}
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("can place only %d of %d replicas (need %.1f CPU, %d MB RAM each)",
				i, n, service.RequiredCPU, service.RequiredRAM)
		}
		node := s.lb.SelectNode(candidates, service)
		if node == nil {
			return nil, fmt.Errorf("load balancer returned no node")
		}
		node.UsedCPU += service.RequiredCPU
		node.UsedRAM += service.RequiredRAM
		node.Containers = append(node.Containers, domain.Container{ServiceID: service.ID, Status: domain.ContainerScheduled})
		for _, p := range service.Ports {
			usedPorts[node.ID][p] = true
		}
		plan = append(plan, node)
	}
	return plan, nil
}

// Schedule finds the best node for a single replica of the service.
func (s *Scheduler) Schedule(service *domain.Service) (*domain.Node, error) {
	plan, err := s.Plan(service, 1)
	if err != nil {
		return nil, err
	}
	node := plan[0]
	log.Printf("scheduler: selected node %s for service %s (%.1f CPU, %d MB RAM left after placement)",
		node.ID, service.Name, node.AvailableCPU(), node.AvailableRAM())
	return node, nil
}

// portConflict reports whether one of the requested ports is already used.
// Each port can be bound only once per machine.
func portConflict(used map[int]bool, ports []int) bool {
	for _, p := range ports {
		if used[p] {
			return true
		}
	}
	return false
}
