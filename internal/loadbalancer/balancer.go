package loadbalancer

import "github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"

// LoadBalancer selects the best node from a list of candidates.
type LoadBalancer interface {
	SelectNode(candidates []*domain.Node, service *domain.Service) *domain.Node
}
