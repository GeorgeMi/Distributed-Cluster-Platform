package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"strings"
	"os"
	"os/signal"
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
	raftPeers := flag.String("raft-peers", "", "comma-separated list of peer addresses (id:addr,id:addr)")
	flag.Parse()

	log.Printf("master starting, api=%s, heartbeats=%s, lb=%s, raft=%s", *apiAddr, *multicast, *lbStrategy, *raftAddr)

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

	// Raft consensus
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
	// JWT manager
	jwtManager := security.NewJWTManager("super-secret-key", 15*time.Minute, 24*time.Hour)

	// Default users
	state.AddUser(&domain.User{ID: "u1", Username: "admin", Role: domain.RoleAdmin, TokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("admin")))})
	state.AddUser(&domain.User{ID: "u2", Username: "writer", Role: domain.RoleWriter, TokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("writer")))})
	state.AddUser(&domain.User{ID: "u3", Username: "reader", Role: domain.RoleReader, TokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("reader")))})

	// Default resource pools
	state.AddPool(&domain.ResourcePool{ID: "high-cpu", Name: "high-cpu", MinCPU: 4, MaxCPU: 128, MinRAM: 0, MaxRAM: 32768})
	state.AddPool(&domain.ResourcePool{ID: "high-ram", Name: "high-ram", MinCPU: 0, MaxCPU: 128, MinRAM: 32769, MaxRAM: 1048576})

	// Load balancer
	var lb loadbalancer.LoadBalancer
	switch *lbStrategy {
	case "weighted":
		lb = &loadbalancer.WeightedLB{}
	default:
		lb = &loadbalancer.LeastConnectionsLB{}
	}

	// Scheduler
	sched := scheduler.NewScheduler(state, lb)

	// Audit logger
	auditLog := audit.NewLogger(state)

	// Fault tolerance with recovery via scheduler
	ft := fault.NewTolerance(
		state,
		5*time.Second,  // check every 5s
		15*time.Second, // suspect after 15s
		30*time.Second, // dead after 30s
		func(nodeID string, containers []*domain.Container) {
			log.Printf("recovery: node %s died with %d container(s)", nodeID, len(containers))
			auditLog.Log("system", audit.EventNodeDead, nodeID, fmt.Sprintf("node died with %d containers", len(containers)))
			// Node status already set by fault tolerance; no duplicate Raft apply needed
			for _, c := range containers {
				// Mark container as rescheduling
				state.SetContainerStatus(c.ID, domain.ContainerRescheduling)

				svc, ok := state.GetService(c.ServiceID)
				if !ok {
					log.Printf("recovery: service %s not found, skipping container %s", c.ServiceID, c.ID)
					continue
				}
				node, err := sched.Schedule(svc)
				if err != nil {
					log.Printf("recovery: cannot reschedule service %s: %v", svc.Name, err)
					state.SetContainerStatus(c.ID, domain.ContainerPending)
					continue
				}
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
					log.Printf("recovery: failed to start container on %s: %v", node.ID, err)
					state.SetContainerStatus(c.ID, domain.ContainerPending)
					continue
				}
				// Mark old container as stopped
				state.SetContainerStatus(c.ID, domain.ContainerStopped)
				// Create new container entry on the new node
				newContainer := &domain.Container{
					ID:        fmt.Sprintf("ctr-%d", time.Now().UnixNano()),
					ServiceID: svc.ID,
					NodeID:    node.ID,
					DockerID:  resp.ContainerID,
					Status:    domain.ContainerRunning,
				}
				if raftNode.IsLeader() {
					raftNode.Apply(cluster.LogAddContainer, newContainer)
				} else {
					state.AddContainer(newContainer)
				}
				auditLog.Log("system", audit.EventContainerRescheduled, c.ID, fmt.Sprintf("rescheduled to node %s as %s", node.ID, newContainer.ID))
				log.Printf("recovery: container %s rescheduled to node %s as %s", c.ID, node.ID, newContainer.ID)
			}
		},
	)
	go ft.Start()

	// Heartbeat receiver with split-brain handler
	receiver := heartbeat.NewReceiver(*multicast, state, ft.HandleLateHeartbeat, func(nodeID string) {
		auditLog.Log("system", audit.EventNodeDiscovered, nodeID, "node discovered via first heartbeat")
	})
	go receiver.Start()

	// REST API
	apiServer := api.NewServer(*apiAddr, state, sched, jwtManager, auditLog, raftNode)
	go apiServer.Start()

	// Print cluster status every 10s
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
