package audit

import (
	"log"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

// Event types
const (
	EventNodeDiscovered      = "NODE_DISCOVERED"
	EventNodeDead            = "NODE_DEAD"
	EventServiceCreate       = "SERVICE_CREATE"
	EventServiceDelete       = "SERVICE_DELETE"
	EventContainerRescheduled = "CONTAINER_RESCHEDULED"
	EventAuthDenied          = "AUTH_DENIED"
	EventLogin               = "LOGIN"
)

// Logger writes audit entries to the cluster state.
type Logger struct {
	state *cluster.State
}

func NewLogger(state *cluster.State) *Logger {
	return &Logger{state: state}
}

// Log appends an audit entry.
func (l *Logger) Log(userID, action, resource, details string) {
	entry := domain.AuditEntry{
		Timestamp: time.Now(),
		UserID:    userID,
		Action:    action,
		Resource:  resource,
		Details:   details,
	}
	l.state.AppendAudit(entry)
	log.Printf("audit: [%s] user=%s resource=%s details=%s", action, userID, resource, details)
}
