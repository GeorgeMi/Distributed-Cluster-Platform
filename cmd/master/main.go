package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/agent"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/api"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/audit"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/fault"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/heartbeat"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/loadbalancer"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/scheduler"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/security"
)

func main() {
	multicast := flag.String("multicast", "239.0.0.1:9090", "multicast address for heartbeats")
	lbStrategy := flag.String("lb", "leastconn", "load balancer strategy: leastconn or weighted")
	apiAddr := flag.String("api", ":8090", "REST API listen address")
	raftAddr := flag.String("raft-addr", "localhost:9000", "Raft bind address")
	raftID := flag.String("raft-id", "master-1", "Raft node ID")
	raftDir := flag.String("raft-dir", "/tmp/dcp-raft", "Raft data directory")
	raftBootstrap := flag.Bool("raft-bootstrap", false, "bootstrap Raft cluster")
	raftPeers := flag.String("raft-peers", "", "comma-separated list of peer addresses (id=host:port,...)")
	peersAPI := flag.String("peers-api", "", "comma-separated list of master API addresses (id=host:port,...) used to forward write requests to the leader")
	flag.Parse()

	log.Printf("master starting, api=%s, heartbeats=%s, lb=%s, raft=%s", *apiAddr, *multicast, *lbStrategy, *raftAddr)

	if *raftBootstrap {
		if err := os.RemoveAll(*raftDir); err != nil {
			log.Printf("warning: could not clean raft dir %s: %v", *raftDir, err)
		}
	}

	state := cluster.NewState()

	// Parse Raft peers (format: "id=host:port,id=host:port")
	var peers []cluster.RaftPeer
	if *raftPeers != "" {
		for _, p := range strings.Split(*raftPeers, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				peers = append(peers, cluster.RaftPeer{ID: parts[0], Address: parts[1]})
			}
		}
	}

	raftNode, err := cluster.NewRaftNode(cluster.RaftConfig{
		NodeID:    *raftID,
		BindAddr:  *raftAddr,
		DataDir:   *raftDir + "/" + *raftID,
		Bootstrap: *raftBootstrap,
		Peers:     peers,
	}, state)
	if err != nil {
		log.Fatalf("raft: %v", err)
	}
	jwtManager := security.NewJWTManager("super-secret-key", 15*time.Minute, 24*time.Hour)

	state.AddUser(&domain.User{ID: "u1", Username: "admin", Role: domain.RoleAdmin, TokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("admin")))})
	state.AddUser(&domain.User{ID: "u2", Username: "writer", Role: domain.RoleWriter, TokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("writer")))})
	state.AddUser(&domain.User{ID: "u3", Username: "reader", Role: domain.RoleReader, TokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("reader")))})

	// Default resource pools (disjoint ranges, so every node matches exactly one pool)
	state.AddPool(&domain.ResourcePool{ID: "general", Name: "general", MinCPU: 0, MaxCPU: 3, MinRAM: 0, MaxRAM: 32768})
	state.AddPool(&domain.ResourcePool{ID: "high-cpu", Name: "high-cpu", MinCPU: 4, MaxCPU: 128, MinRAM: 0, MaxRAM: 32768})
	state.AddPool(&domain.ResourcePool{ID: "high-ram", Name: "high-ram", MinCPU: 0, MaxCPU: 128, MinRAM: 32769, MaxRAM: 1048576})

	var lb loadbalancer.LoadBalancer
	switch *lbStrategy {
	case "weighted":
		lb = &loadbalancer.WeightedLB{}
	default:
		lb = &loadbalancer.LeastConnectionsLB{}
	}

	sched := scheduler.NewScheduler(state, lb)

	auditLog := audit.NewLogger(state)

	// Replicated container status updates (leader only)
	applyContainerStatus := func(containerID, status string) {
		if err := raftNode.Apply(cluster.LogSetContainerStatus, map[string]string{"id": containerID, "status": status}); err != nil {
			log.Printf("raft: apply SetContainerStatus: %v", err)
		}
	}

	startOn := func(svc *domain.Service, node *domain.Node) (string, error) {
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
			return "", err
		}
		if !resp.Success {
			return "", fmt.Errorf("worker %s: %s", node.ID, resp.Error)
		}
		return resp.ContainerID, nil
	}

	startReplica := func(svc *domain.Service) (string, string, error) {
		node, err := sched.Schedule(svc)
		if err != nil {
			return "", "", err
		}
		dockerID, err := startOn(svc, node)
		if err != nil {
			return "", "", err
		}
		return node.ID, dockerID, nil
	}

	ft := fault.NewTolerance(
		state,
		5*time.Second,  // check every 5s
		15*time.Second, // suspect after 15s
		30*time.Second, // dead after 30s
		raftNode.IsLeader,
		applyContainerStatus,
		func(nodeID string, containers []*domain.Container) {
			if !raftNode.IsLeader() {
				return
			}
			log.Printf("recovery: node %s died with %d container(s)", nodeID, len(containers))
			auditLog.Log("system", audit.EventNodeDead, nodeID, fmt.Sprintf("node died with %d containers", len(containers)))
			for _, c := range containers {
				applyContainerStatus(c.ID, domain.ContainerRescheduling)

				svc, ok := state.GetService(c.ServiceID)
				if !ok {
					log.Printf("recovery: service %s not found, skipping container %s", c.ServiceID, c.ID)
					continue
				}
				newNodeID, dockerID, err := startReplica(svc)
				if err != nil {
					log.Printf("recovery: cannot reschedule service %s: %v", svc.Name, err)
					applyContainerStatus(c.ID, domain.ContainerPending)
					continue
				}
				applyContainerStatus(c.ID, domain.ContainerStopped)
				newContainer := &domain.Container{
					ID:        fmt.Sprintf("ctr-%d", time.Now().UnixNano()),
					ServiceID: svc.ID,
					NodeID:    newNodeID,
					DockerID:  dockerID,
					Status:    domain.ContainerRunning,
				}
				if err := raftNode.Apply(cluster.LogAddContainer, newContainer); err != nil {
					log.Printf("recovery: apply AddContainer: %v", err)
					continue
				}
				auditLog.Log("system", audit.EventContainerRescheduled, c.ID, fmt.Sprintf("rescheduled to node %s as %s", newNodeID, newContainer.ID))
				log.Printf("recovery: container %s rescheduled to node %s as %s", c.ID, newNodeID, newContainer.ID)
			}
		},
	)
	go ft.Start()

	// Retry loop: PENDING containers of a service are scheduled again, all-or-nothing.
	go func() {
		for {
			time.Sleep(10 * time.Second)
			if !raftNode.IsLeader() {
				continue
			}
			byService := make(map[string][]*domain.Container)
			for _, c := range state.GetContainersByStatus(domain.ContainerPending) {
				byService[c.ServiceID] = append(byService[c.ServiceID], c)
			}
			for svcID, group := range byService {
				svc, ok := state.GetService(svcID)
				if !ok {
					continue
				}
				plan, err := sched.Plan(svc, len(group))
				if err != nil {
					continue
				}
				for i, c := range group {
					node := plan[i]
					dockerID, err := startOn(svc, node)
					if err != nil {
						log.Printf("retry: failed to start container %s: %v", c.ID, err)
						continue
					}
					updated := &domain.Container{
						ID:        c.ID,
						ServiceID: c.ServiceID,
						NodeID:    node.ID,
						DockerID:  dockerID,
						Status:    domain.ContainerRunning,
					}
					if err := raftNode.Apply(cluster.LogAddContainer, updated); err != nil {
						log.Printf("retry: apply AddContainer: %v", err)
						continue
					}
					log.Printf("retry: pending container %s started on node %s", c.ID, node.ID)
				}
			}
		}
	}()

	receiver := heartbeat.NewReceiver(*multicast, state, ft.HandleLateHeartbeat, func(nodeID string) {
		auditLog.Log("system", audit.EventNodeDiscovered, nodeID, "node discovered via first heartbeat")
	})
	go receiver.Start()

	// API addresses of all masters (raft ID -> host:port), for leader forwarding
	apiPeers := make(map[string]string)
	if *peersAPI != "" {
		for _, p := range strings.Split(*peersAPI, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				apiPeers[parts[0]] = parts[1]
			}
		}
	}

	apiServer := api.NewServer(*apiAddr, state, sched, jwtManager, auditLog, raftNode, apiPeers)
	go apiServer.Start()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			nodes := state.GetAllNodes()
			if len(nodes) == 0 {
				log.Println("cluster status: no nodes")
				continue
			}
			log.Printf("cluster status: %d node(s)", len(nodes))
			for _, n := range nodes {
				since := time.Since(n.LastHeartbeat).Round(time.Second)
				log.Printf("  %s [%s] CPU=%.1f/%.1f RAM=%d/%dMB last_hb=%s ago",
					n.ID, n.Status, n.UsedCPU, n.TotalCPU, n.UsedRAM, n.TotalRAM, since)
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("master shutting down")
	receiver.Stop()
	ft.Stop()
}
