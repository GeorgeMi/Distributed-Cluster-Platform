package loadbalancer

import "github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"

// WeightedLB picks the node with the highest combined free-CPU and free-RAM score.
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

// score normalizes free CPU and free RAM to a 0-1 range so they weigh equally.
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
