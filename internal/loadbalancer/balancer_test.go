package loadbalancer

import (
	"testing"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

func makeNode(id string, totalCPU, usedCPU float64, totalRAM, usedRAM int64, containers int) *domain.Node {
	n := &domain.Node{
		ID:       id,
		Status:   domain.NodeAlive,
		TotalCPU: totalCPU,
		UsedCPU:  usedCPU,
		TotalRAM: totalRAM,
		UsedRAM:  usedRAM,
	}
	for i := 0; i < containers; i++ {
		n.Containers = append(n.Containers, domain.Container{ID: "c"})
	}
	return n
}

func TestLeastConnections_SelectsNodeWithFewestContainers(t *testing.T) {
	lb := &LeastConnectionsLB{}
	nodes := []*domain.Node{
		makeNode("n1", 8, 2, 16384, 4096, 5),
		makeNode("n2", 8, 2, 16384, 4096, 2),
		makeNode("n3", 8, 2, 16384, 4096, 8),
	}

	result := lb.SelectNode(nodes, &domain.Service{})
	if result.ID != "n2" {
		t.Errorf("expected n2 (2 containers), got %s", result.ID)
	}
}

func TestLeastConnections_EmptyCandidates(t *testing.T) {
	lb := &LeastConnectionsLB{}
	result := lb.SelectNode(nil, &domain.Service{})
	if result != nil {
		t.Error("expected nil for empty candidates")
	}
}

func TestWeighted_SelectsNodeWithMostResources(t *testing.T) {
	lb := &WeightedLB{}
	nodes := []*domain.Node{
		makeNode("n1", 8, 6, 16384, 12000, 0), // 25% CPU free, 26% RAM free
		makeNode("n2", 8, 1, 16384, 2000, 0),  // 87% CPU free, 87% RAM free
		makeNode("n3", 8, 4, 16384, 8000, 0),  // 50% CPU free, 51% RAM free
	}

	result := lb.SelectNode(nodes, &domain.Service{})
	if result.ID != "n2" {
		t.Errorf("expected n2 (most resources), got %s", result.ID)
	}
}

func TestWeighted_EmptyCandidates(t *testing.T) {
	lb := &WeightedLB{}
	result := lb.SelectNode(nil, &domain.Service{})
	if result != nil {
		t.Error("expected nil for empty candidates")
	}
}
