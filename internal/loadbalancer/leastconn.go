package loadbalancer

import "github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"

// LeastConnectionsLB picks the node with the fewest running containers.
type LeastConnectionsLB struct{}

func (lb *LeastConnectionsLB) SelectNode(candidates []*domain.Node, service *domain.Service) *domain.Node {
	if len(candidates) == 0 {
		return nil
	}

	best := candidates[0]
	for _, n := range candidates[1:] {
		if len(n.Containers) < len(best.Containers) {
			best = n
		}
	}
	return best
}
