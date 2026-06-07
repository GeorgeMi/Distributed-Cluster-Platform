package api

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/agent"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/audit"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/scheduler"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/security"
)

type Server struct {
	state     *cluster.State
	scheduler *scheduler.Scheduler
	jwt       *security.JWTManager
	audit     *audit.Logger
	raftNode  *cluster.RaftNode
	addr      string
}

func NewServer(addr string, state *cluster.State, sched *scheduler.Scheduler, jwt *security.JWTManager, auditLog *audit.Logger, raftNode *cluster.RaftNode) *Server {
	return &Server{
		addr:      addr,
		state:     state,
		scheduler: sched,
		jwt:       jwt,
		audit:     auditLog,
		raftNode:  raftNode,
	}
}

func (s *Server) Start() {
	mux := http.NewServeMux()

	// Public endpoints
	mux.HandleFunc("POST /login", s.login)
	mux.HandleFunc("POST /refresh", s.refresh)

	// Protected endpoints
	mux.HandleFunc("GET /nodes", s.auth("nodes", "read", s.getNodes))
	mux.HandleFunc("GET /pools", s.auth("pools", "read", s.getPools))
	mux.HandleFunc("GET /services", s.auth("services", "read", s.getServices))
	mux.HandleFunc("POST /services", s.auth("services", "create", s.createService))
	mux.HandleFunc("DELETE /services/{id}", s.auth("services", "delete", s.deleteService))
	mux.HandleFunc("GET /audit", s.auth("audit", "read", s.getAudit))

	log.Printf("api: listening on %s", s.addr)
	if err := http.ListenAndServe(s.addr, mux); err != nil {
		log.Fatalf("api: %v", err)
	}
}

// auth is a middleware that checks JWT and ACL permissions.
func (s *Server) auth(resource, action string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract token from Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or invalid Authorization header"})
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		// Validate token
		claims, err := s.jwt.ValidateAccessToken(tokenStr)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token: " + err.Error()})
			return
		}

		// Check ACL
		if !security.CheckPermission(claims.Role, resource, action) {
			s.audit.Log(claims.UserID, audit.EventAuthDenied, resource, fmt.Sprintf("role %s cannot %s %s", claims.Role, action, resource))
			writeJSON(w, http.StatusForbidden, map[string]string{"error": fmt.Sprintf("role %s cannot %s %s", claims.Role, action, resource)})
			return
		}

		next(w, r)
	}
}

// --- Auth endpoints ---

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// POST /login - authenticate and get tokens
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	user, ok := s.state.GetUserByUsername(req.Username)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	// Check password hash
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(req.Password)))
	if hash != user.TokenHash {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	tokens, err := s.jwt.GenerateTokenPair(user.ID, user.Role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to generate token"})
		return
	}

	s.audit.Log(user.ID, audit.EventLogin, "auth", fmt.Sprintf("user %s logged in", user.Username))
	writeJSON(w, http.StatusOK, tokens)
}

type RefreshRequest struct {
	RefreshToken string `json:"refreshToken"`
}

// POST /refresh - get new access token using refresh token
func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var req RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}

	userID, err := s.jwt.ValidateRefreshToken(req.RefreshToken)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid refresh token"})
		return
	}

	// Find user to get current role
	var user *domain.User
	users := s.state.GetAllUsers()
	for _, u := range users {
		if u.ID == userID {
			user = u
			break
		}
	}
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "user not found"})
		return
	}

	tokens, err := s.jwt.GenerateTokenPair(user.ID, user.Role)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to generate token"})
		return
	}

	writeJSON(w, http.StatusOK, tokens)
}

// --- Resource endpoints ---

// GET /nodes
func (s *Server) getNodes(w http.ResponseWriter, r *http.Request) {
	nodes := s.state.GetAllNodes()
	writeJSON(w, http.StatusOK, nodes)
}

// GET /pools
func (s *Server) getPools(w http.ResponseWriter, r *http.Request) {
	pools := s.state.GetAllPools()
	writeJSON(w, http.StatusOK, pools)
}

// GET /services
func (s *Server) getServices(w http.ResponseWriter, r *http.Request) {
	services := s.state.GetAllServices()
	writeJSON(w, http.StatusOK, services)
}

type CreateServiceRequest struct {
	Name        string            `json:"name"`
	Image       string            `json:"image"`
	RequiredCPU float64           `json:"cpu"`
	RequiredRAM int64             `json:"ram"`
	Replicas    int               `json:"replicas"`
	PoolID      string            `json:"pool"`
	EnvVars     map[string]string `json:"envVars,omitempty"`
	Ports       []int             `json:"ports,omitempty"`
	Cmd         []string          `json:"cmd,omitempty"`
}

// POST /services
func (s *Server) createService(w http.ResponseWriter, r *http.Request) {
	var req CreateServiceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	if req.Image == "" || req.Replicas <= 0 || req.PoolID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "image, replicas, and pool are required"})
		return
	}

	svc := &domain.Service{
		ID:              generateID("svc"),
		Name:            req.Name,
		Image:           req.Image,
		RequiredCPU:     req.RequiredCPU,
		RequiredRAM:     req.RequiredRAM,
		DesiredReplicas: req.Replicas,
		PoolID:          req.PoolID,
		EnvVars:         req.EnvVars,
		Ports:           req.Ports,
		Cmd:             req.Cmd,
	}
	if s.raftNode != nil {
		s.raftNode.Apply(cluster.LogAddService, svc)
	} else {
		s.state.AddService(svc)
	}

	var containers []map[string]string
	for i := 0; i < req.Replicas; i++ {
		node, err := s.scheduler.Schedule(svc)
		if err != nil {
			log.Printf("api: cannot schedule replica %d of %s: %v", i+1, svc.Name, err)
			c := &domain.Container{
				ID:        generateID("ctr"),
				ServiceID: svc.ID,
				Status:    domain.ContainerPending,
			}
			s.applyAddContainer(c)
			containers = append(containers, map[string]string{"id": c.ID, "status": domain.ContainerPending})
			continue
		}

		// Container scheduled - node found, waiting for Docker to start
		c := &domain.Container{
			ID:        generateID("ctr"),
			ServiceID: svc.ID,
			NodeID:    node.ID,
			Status:    domain.ContainerScheduled,
		}
		s.applyAddContainer(c)

		resp, err := agent.SendCommand(node.Address, agent.Command{
			Type:      agent.CmdStartContainer,
			ServiceID: svc.ID,
			Image:     svc.Image,
			CPULimit:  svc.RequiredCPU,
			RAMLimit:  svc.RequiredRAM,
			EnvVars:   svc.EnvVars,
			Ports:     svc.Ports,
			Cmd:       svc.Cmd,
		})
		if err != nil {
			log.Printf("api: failed to start container on %s: %v", node.ID, err)
			s.state.SetContainerStatus(c.ID, domain.ContainerPending)
			containers = append(containers, map[string]string{"id": c.ID, "status": domain.ContainerPending})
			continue
		}

		if !resp.Success {
			log.Printf("api: worker %s returned error: %s", node.ID, resp.Error)
			s.state.SetContainerStatus(c.ID, domain.ContainerFailed)
			containers = append(containers, map[string]string{"id": c.ID, "status": domain.ContainerFailed, "error": resp.Error})
			continue
		}

		c.DockerID = resp.ContainerID
		c.Status = domain.ContainerRunning
		svc.Replicas++
		containers = append(containers, map[string]string{"id": c.ID, "nodeID": node.ID, "status": domain.ContainerRunning})
		log.Printf("api: service %s replica %d started on node %s", svc.Name, i+1, node.ID)
	}

	s.audit.Log("", audit.EventServiceCreate, svc.ID, fmt.Sprintf("service %s created with %d replicas", svc.Name, req.Replicas))

	writeJSON(w, http.StatusCreated, map[string]any{
		"serviceID":  svc.ID,
		"name":       svc.Name,
		"containers": containers,
	})
}

// DELETE /services/{id}
func (s *Server) deleteService(w http.ResponseWriter, r *http.Request) {
	serviceID := r.PathValue("id")

	svc, ok := s.state.GetService(serviceID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "service not found"})
		return
	}

	containers := s.state.GetContainersByService(serviceID)
	for _, c := range containers {
		if c.NodeID != "" {
			node, ok := s.state.GetNode(c.NodeID)
			if ok {
				agent.SendCommand(node.Address, agent.Command{
					Type:      agent.CmdKillContainers,
					ServiceID: serviceID,
				})
			}
		}
		s.state.SetContainerStatus(c.ID, domain.ContainerStopped)
	}

	s.applyRemoveService(serviceID)
	s.audit.Log("", audit.EventServiceDelete, serviceID, fmt.Sprintf("service %s deleted", svc.Name))
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// GET /audit?action=...&user=...
func (s *Server) getAudit(w http.ResponseWriter, r *http.Request) {
	actionFilter := r.URL.Query().Get("action")
	userFilter := r.URL.Query().Get("user")

	entries := s.state.GetAuditLog(func(e domain.AuditEntry) bool {
		if actionFilter != "" && e.Action != actionFilter {
			return false
		}
		if userFilter != "" && e.UserID != userFilter {
			return false
		}
		return true
	})
	writeJSON(w, http.StatusOK, entries)
}

// applyAddContainer adds a container through Raft or directly.
func (s *Server) applyAddContainer(c *domain.Container) {
	if s.raftNode != nil {
		s.raftNode.Apply(cluster.LogAddContainer, c)
	} else {
		s.state.AddContainer(c)
	}
}

// applyRemoveService removes a service through Raft or directly.
func (s *Server) applyRemoveService(id string) {
	if s.raftNode != nil {
		s.raftNode.Apply(cluster.LogRemoveService, map[string]string{"id": id})
	} else {
		s.state.RemoveService(id)
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func generateID(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, randomString(8))
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
