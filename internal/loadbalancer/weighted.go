package loadbalancer

import "github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"

// WeightedLB picks the node with the most available resources.
// Score = available CPU + available RAM (normalized to CPU units).
type WeightedLB struct{}

func (lb *WeightedLB) SelectNode(candidates []*domain.Node, service *domain.Service) *domain.Node {
	if len(candidates) == 0 {
		return nil
	}

	best := candidates[0]
	bestScore := score(best)
	for _, n := range candidates[1:] {
		s := score(n)
		if s > bestScore {
			best = n
			bestScore = s
		}
	}
	return best
}

// score calculates a simple weight: available CPU + available RAM in GB.
func score(n *domain.Node) float64 {
	return n.AvailableCPU() + float64(n.AvailableRAM())/1024.0
}
