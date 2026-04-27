package domain

import "time"

// Statuses
const (
	NodeAlive             = "ALIVE"
	NodeSuspect           = "SUSPECT"
	NodeDead              = "DEAD"
	NodeRemoved           = "REMOVED"
	ContainerPending      = "PENDING"
	ContainerScheduled    = "SCHEDULED"
	ContainerRunning      = "RUNNING"
	ContainerFailed       = "FAILED"
	ContainerRescheduling = "RESCHEDULING"
	ContainerStopped      = "STOPPED"
)

// User roles
const (
	RoleAdmin  = "ADMIN"
	RoleWriter = "WRITER"
	RoleReader = "READER"
)

type Node struct {
	ID            string
	Address       string
	Status        string
	TotalCPU      float64
	UsedCPU       float64
	TotalRAM      int64
	UsedRAM       int64
	LastHeartbeat time.Time
	Containers    []Container
}

func (n *Node) IsAlive() bool {
	return n.Status == NodeAlive
}

func (n *Node) AvailableCPU() float64 {
	return n.TotalCPU - n.UsedCPU
}

func (n *Node) AvailableRAM() int64 {
	return n.TotalRAM - n.UsedRAM
}

type Container struct {
	ID        string
	ServiceID string
	NodeID    string
	DockerID  string
	Status    string
	StartedAt time.Time
}

type Service struct {
	ID              string
	Name            string
	Image           string
	RequiredCPU     float64
	RequiredRAM     int64
	Replicas        int
	DesiredReplicas int
	PoolID          string
	Containers      []Container
	CreatedBy       string
	EnvVars         map[string]string
	Ports           []int
	Cmd             []string
}

type ResourcePool struct {
	ID      string
	Name    string
	MinCPU  float64
	MaxCPU  float64
	MinRAM  int64
	MaxRAM  int64
	NodeIDs []string
}

func (p *ResourcePool) MatchesNode(n Node) bool {
	return n.TotalCPU >= p.MinCPU && n.TotalCPU <= p.MaxCPU &&
		n.TotalRAM >= p.MinRAM && n.TotalRAM <= p.MaxRAM
}

func (p *ResourcePool) AvailableCapacity(nodes []Node) (float64, int64) {
	var cpu float64
	var ram int64
	for _, n := range nodes {
		if n.IsAlive() {
			cpu += n.AvailableCPU()
			ram += n.AvailableRAM()
		}
	}
	return cpu, ram
}

type AuditEntry struct {
	Timestamp time.Time
	UserID    string
	Action    string
	Resource  string
	Details   string
}

type User struct {
	ID        string
	Username  string
	Role      string
	TokenHash string
}
