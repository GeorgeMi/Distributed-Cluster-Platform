package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

type State struct {
	mu         sync.RWMutex
	nodes      map[string]*domain.Node
	services   map[string]*domain.Service
	containers map[string]*domain.Container
	pools      map[string]*domain.ResourcePool
	users      map[string]*domain.User
	audit      []domain.AuditEntry
}

func NewState() *State {
	return &State{
		nodes:      make(map[string]*domain.Node),
		services:   make(map[string]*domain.Service),
		containers: make(map[string]*domain.Container),
		pools:      make(map[string]*domain.ResourcePool),
		users:      make(map[string]*domain.User),
	}
}

// --- Nodes ---

func (s *State) AddNode(node *domain.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[node.ID] = node
}

func (s *State) GetNode(id string) (*domain.Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	return n, ok
}

func (s *State) GetAllNodes() []*domain.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*domain.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		result = append(result, n)
	}
	return result
}

func (s *State) RemoveNode(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, id)
}

func (s *State) UpdateNodeMetrics(nodeID string, cpuUsed float64, ramUsed int64, activeConns int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	n.UsedCPU = cpuUsed
	n.UsedRAM = ramUsed
	n.ActiveConnections = activeConns
	n.LastHeartbeat = time.Now()
	n.Status = domain.NodeAlive
	return nil
}

func (s *State) SetNodeStatus(nodeID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	n.Status = status
	return nil
}

// --- Services ---

func (s *State) AddService(svc *domain.Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services[svc.ID] = svc
}

func (s *State) GetService(id string) (*domain.Service, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	svc, ok := s.services[id]
	return svc, ok
}

func (s *State) GetAllServices() []*domain.Service {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*domain.Service, 0, len(s.services))
	for _, svc := range s.services {
		result = append(result, svc)
	}
	return result
}

func (s *State) RemoveService(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.services, id)
}

func (s *State) SetServiceReplicas(serviceID string, replicas int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	svc, ok := s.services[serviceID]
	if !ok {
		return fmt.Errorf("service %s not found", serviceID)
	}
	svc.Replicas = replicas
	return nil
}

// --- Containers ---

func (s *State) AddContainer(c *domain.Container) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.containers[c.ID] = c
}

func (s *State) GetContainer(id string) (*domain.Container, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.containers[id]
	return c, ok
}

func (s *State) GetContainersByNode(nodeID string) []*domain.Container {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*domain.Container
	for _, c := range s.containers {
		if c.NodeID == nodeID {
			result = append(result, c)
		}
	}
	return result
}

func (s *State) GetActiveContainersByNode(nodeID string) []domain.Container {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []domain.Container
	for _, c := range s.containers {
		if c.NodeID == nodeID && (c.Status == domain.ContainerRunning || c.Status == domain.ContainerScheduled) {
			result = append(result, *c)
		}
	}
	return result
}

func (s *State) GetContainersByService(serviceID string) []*domain.Container {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*domain.Container
	for _, c := range s.containers {
		if c.ServiceID == serviceID {
			result = append(result, c)
		}
	}
	return result
}

func (s *State) RemoveContainer(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.containers, id)
}

func (s *State) SetContainerStatus(containerID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.containers[containerID]
	if !ok {
		return fmt.Errorf("container %s not found", containerID)
	}
	c.Status = status
	return nil
}

// --- Resource Pools ---

func (s *State) AddPool(pool *domain.ResourcePool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pools[pool.ID] = pool
}

func (s *State) GetPool(id string) (*domain.ResourcePool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pools[id]
	return p, ok
}

func (s *State) GetAllPools() []*domain.ResourcePool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*domain.ResourcePool, 0, len(s.pools))
	for _, p := range s.pools {
		result = append(result, p)
	}
	return result
}

func (s *State) AssignNodeToPool(nodeID string, node *domain.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pool := range s.pools {
		if pool.MatchesNode(*node) {
			for _, existing := range pool.NodeIDs {
				if existing == nodeID {
					return
				}
			}
			pool.NodeIDs = append(pool.NodeIDs, nodeID)
			return
		}
	}
}

func (s *State) GetPoolNodes(poolID string) []*domain.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pool, ok := s.pools[poolID]
	if !ok {
		return nil
	}
	var result []*domain.Node
	for _, nid := range pool.NodeIDs {
		if n, ok := s.nodes[nid]; ok {
			result = append(result, n)
		}
	}
	return result
}

// --- Users ---

func (s *State) AddUser(user *domain.User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[user.ID] = user
}

func (s *State) GetAllUsers() []*domain.User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*domain.User, 0, len(s.users))
	for _, u := range s.users {
		result = append(result, u)
	}
	return result
}

func (s *State) GetUserByUsername(username string) (*domain.User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		if u.Username == username {
			return u, true
		}
	}
	return nil, false
}

// --- Audit ---

func (s *State) AppendAudit(entry domain.AuditEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = append(s.audit, entry)
}

func (s *State) GetAuditLog(filter func(domain.AuditEntry) bool) []domain.AuditEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []domain.AuditEntry
	for _, e := range s.audit {
		if filter == nil || filter(e) {
			result = append(result, e)
		}
	}
	return result
}

type Snapshot struct {
	Nodes      []*domain.Node         `json:"nodes"`
	Services   []*domain.Service      `json:"services"`
	Containers []*domain.Container    `json:"containers"`
	Pools      []*domain.ResourcePool `json:"pools"`
	Users      []*domain.User         `json:"users"`
	Audit      []domain.AuditEntry    `json:"audit"`
}

func (s *State) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{Audit: append([]domain.AuditEntry(nil), s.audit...)}
	for _, n := range s.nodes {
		snap.Nodes = append(snap.Nodes, n)
	}
	for _, svc := range s.services {
		snap.Services = append(snap.Services, svc)
	}
	for _, c := range s.containers {
		snap.Containers = append(snap.Containers, c)
	}
	for _, p := range s.pools {
		snap.Pools = append(snap.Pools, p)
	}
	for _, u := range s.users {
		snap.Users = append(snap.Users, u)
	}
	return snap
}

func (s *State) Restore(snap Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = make(map[string]*domain.Node, len(snap.Nodes))
	for _, n := range snap.Nodes {
		s.nodes[n.ID] = n
	}
	s.services = make(map[string]*domain.Service, len(snap.Services))
	for _, svc := range snap.Services {
		s.services[svc.ID] = svc
	}
	s.containers = make(map[string]*domain.Container, len(snap.Containers))
	for _, c := range snap.Containers {
		s.containers[c.ID] = c
	}
	s.pools = make(map[string]*domain.ResourcePool, len(snap.Pools))
	for _, p := range snap.Pools {
		s.pools[p.ID] = p
	}
	s.users = make(map[string]*domain.User, len(snap.Users))
	for _, u := range snap.Users {
		s.users[u.ID] = u
	}
	s.audit = append([]domain.AuditEntry(nil), snap.Audit...)
}
