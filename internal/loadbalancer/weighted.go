package loadbalancer

import "github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"

// WeightedLB picks the node with the most available resources.
// Score is computed as a weighted score between available CPU and available RAM.
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

// score computes a weighted score between available CPU and available RAM.
// Both are normalized to a 0-1 range (percentage of total) so they contribute equally.
func score(n *domain.Node) float64 {
	var cpuScore, ramScore float64
	if n.TotalCPU > 0 {
		cpuScore = n.AvailableCPU() / n.TotalCPU
	}
	if n.TotalRAM > 0 {
		ramScore = float64(n.AvailableRAM()) / float64(n.TotalRAM)
	}
	return cpuScore + ramScore
}
