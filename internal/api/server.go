package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
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
	peersAPI  map[string]string // raft ID -> API address of each master
}

func NewServer(addr string, state *cluster.State, sched *scheduler.Scheduler, jwt *security.JWTManager, auditLog *audit.Logger, raftNode *cluster.RaftNode, peersAPI map[string]string) *Server {
	return &Server{
		addr:      addr,
		state:     state,
		scheduler: sched,
		jwt:       jwt,
		audit:     auditLog,
		raftNode:  raftNode,
		peersAPI:  peersAPI,
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
	mux.HandleFunc("POST /services", s.auth("services", "create", s.leaderForward(s.createService)))
	mux.HandleFunc("DELETE /services/{id}", s.auth("services", "delete", s.leaderForward(s.deleteService)))
	mux.HandleFunc("GET /audit", s.auth("audit", "read", s.getAudit))

	log.Printf("api: listening on %s", s.addr)
	if err := http.ListenAndServe(s.addr, mux); err != nil {
		log.Fatalf("api: %v", err)
	}
}

// auth is a middleware that checks JWT and ACL permissions.
func (s *Server) auth(resource, action string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or invalid Authorization header"})
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		claims, err := s.jwt.ValidateAccessToken(tokenStr)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token: " + err.Error()})
			return
		}

		if !security.CheckPermission(claims.Role, resource, action) {
			s.audit.Log(claims.UserID, audit.EventAuthDenied, resource, fmt.Sprintf("role %s cannot %s %s", claims.Role, action, resource))
			writeJSON(w, http.StatusForbidden, map[string]string{"error": fmt.Sprintf("role %s cannot %s %s", claims.Role, action, resource)})
			return
		}

		next(w, r.WithContext(context.WithValue(r.Context(), userIDKey, claims.UserID)))
	}
}

type ctxKey string

const userIDKey ctxKey = "userID"

func requestUserID(r *http.Request) string {
	if id, ok := r.Context().Value(userIDKey).(string); ok {
		return id
	}
	return ""
}

// forwardedHeader marks a request already forwarded once, to avoid loops
// when leadership changes while the request is in flight.
const forwardedHeader = "X-DCP-Forwarded"

// leaderForward runs the handler on the leader. On a follower, the request
// is transparently proxied to the leader's API, so users can send write
// requests to any master.
func (s *Server) leaderForward(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.raftNode == nil || s.raftNode.IsLeader() {
			next(w, r)
			return
		}

		leaderRaft, leaderID := s.raftNode.Leader()
		if leaderID == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader elected yet; retry"})
			return
		}
		if r.Header.Get(forwardedHeader) != "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leadership changed while forwarding; retry"})
			return
		}
		apiAddr, ok := s.peersAPI[leaderID]
		if !ok {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error":      "not the leader and no API address known for it; start masters with -peers-api",
				"leaderID":   leaderID,
				"leaderRaft": leaderRaft,
			})
			return
		}

		log.Printf("api: forwarding %s %s to leader %s (%s)", r.Method, r.URL.Path, leaderID, apiAddr)
		r.Header.Set(forwardedHeader, "1")
		proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: apiAddr})
		proxy.ServeHTTP(w, r)
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
	out := make([]*domain.Node, 0, len(nodes))
	for _, n := range nodes {
		view := *n
		view.Containers = s.state.GetActiveContainersByNode(n.ID)
		out = append(out, &view)
	}
	writeJSON(w, http.StatusOK, out)
}

// GET /pools
func (s *Server) getPools(w http.ResponseWriter, r *http.Request) {
	pools := s.state.GetAllPools()
	writeJSON(w, http.StatusOK, pools)
}

// GET /services
func (s *Server) getServices(w http.ResponseWriter, r *http.Request) {
	services := s.state.GetAllServices()
	out := make([]*domain.Service, 0, len(services))
	for _, svc := range services {
		view := *svc
		view.Replicas = 0
		for _, c := range s.state.GetContainersByService(svc.ID) {
			if c.Status == domain.ContainerRunning {
				view.Replicas++
			}
		}
		out = append(out, &view)
	}
	writeJSON(w, http.StatusOK, out)
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
		if err := s.raftNode.Apply(cluster.LogAddService, svc); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "failed to replicate service: " + err.Error()})
			return
		}
	} else {
		s.state.AddService(svc)
	}

	var containers []map[string]string

	// All-or-nothing: if not every replica can be placed, none is started.
	plan, planErr := s.scheduler.Plan(svc, req.Replicas)
	if planErr != nil {
		log.Printf("api: cannot place all %d replicas of %s: %v", req.Replicas, svc.Name, planErr)
		for i := 0; i < req.Replicas; i++ {
			c := &domain.Container{
				ID:        generateID("ctr"),
				ServiceID: svc.ID,
				Status:    domain.ContainerPending,
			}
			s.applyAddContainer(c)
			containers = append(containers, map[string]string{"id": c.ID, "status": domain.ContainerPending})
		}
		s.audit.Log(requestUserID(r), audit.EventServiceCreate, svc.ID, fmt.Sprintf("service %s created with %d replicas (pending)", svc.Name, req.Replicas))
		writeJSON(w, http.StatusCreated, map[string]any{
			"serviceID":  svc.ID,
			"name":       svc.Name,
			"containers": containers,
			"message":    "not enough capacity for all replicas, nothing was started; " + planErr.Error(),
		})
		return
	}

	for i := 0; i < req.Replicas; i++ {
		node := plan[i]

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
			s.applySetContainerStatus(c.ID, domain.ContainerPending)
			containers = append(containers, map[string]string{"id": c.ID, "status": domain.ContainerPending})
			continue
		}

		if !resp.Success {
			log.Printf("api: worker %s returned error: %s", node.ID, resp.Error)
			s.applySetContainerStatus(c.ID, domain.ContainerFailed)
			containers = append(containers, map[string]string{"id": c.ID, "status": domain.ContainerFailed, "error": resp.Error})
			continue
		}

		c.DockerID = resp.ContainerID
		c.Status = domain.ContainerRunning
		s.applyAddContainer(c)
		containers = append(containers, map[string]string{"id": c.ID, "nodeID": node.ID, "status": domain.ContainerRunning})
		log.Printf("api: service %s replica %d started on node %s", svc.Name, i+1, node.ID)
	}

	s.audit.Log(requestUserID(r), audit.EventServiceCreate, svc.ID, fmt.Sprintf("service %s created with %d replicas", svc.Name, req.Replicas))

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
		s.applySetContainerStatus(c.ID, domain.ContainerStopped)
	}

	s.applyRemoveService(serviceID)
	s.audit.Log(requestUserID(r), audit.EventServiceDelete, serviceID, fmt.Sprintf("service %s deleted", svc.Name))
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

// applyAddContainer adds or updates a container through Raft or directly.
func (s *Server) applyAddContainer(c *domain.Container) {
	if s.raftNode != nil {
		if err := s.raftNode.Apply(cluster.LogAddContainer, c); err != nil {
			log.Printf("api: apply AddContainer: %v", err)
		}
	} else {
		s.state.AddContainer(c)
	}
}

func (s *Server) applySetContainerStatus(id, status string) {
	if s.raftNode != nil {
		if err := s.raftNode.Apply(cluster.LogSetContainerStatus, map[string]string{"id": id, "status": status}); err != nil {
			log.Printf("api: apply SetContainerStatus: %v", err)
		}
	} else {
		s.state.SetContainerStatus(id, status)
	}
}

// applyRemoveService removes a service through Raft or directly.
func (s *Server) applyRemoveService(id string) {
	if s.raftNode != nil {
		if err := s.raftNode.Apply(cluster.LogRemoveService, map[string]string{"id": id}); err != nil {
			log.Printf("api: apply RemoveService: %v", err)
		}
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
