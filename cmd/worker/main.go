package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/agent"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/heartbeat"
)

func main() {
	nodeID := flag.String("id", "", "node ID (required)")
	addr := flag.String("addr", "localhost:7000", "node address for commands")
	multicast := flag.String("multicast", "239.0.0.1:9090", "multicast address for heartbeats")
	interval := flag.Duration("interval", 5*time.Second, "heartbeat interval")
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "usage: worker -id <node-id> [-addr host:port] [-multicast addr:port]")
		os.Exit(1)
	}

	log.Printf("worker %s starting, address=%s, multicast=%s", *nodeID, *addr, *multicast)

	// Docker manager
	docker, err := agent.NewDockerManager(*nodeID)
	if err != nil {
		log.Fatalf("docker: %v", err)
	}

	// Command server - receives StartContainer/KillContainers from master
	cmdServer := agent.NewCommandServer(*addr, func(cmd agent.Command) agent.CommandResponse {
		switch cmd.Type {
		case agent.CmdStartContainer:
			log.Printf("worker: StartContainer image=%s service=%s", cmd.Image, cmd.ServiceID)
			containerID, err := docker.StartContainer(cmd)
			if err != nil {
				log.Printf("worker: StartContainer failed: %v", err)
				return agent.CommandResponse{Success: false, Error: err.Error()}
			}
			return agent.CommandResponse{Success: true, ContainerID: containerID}

		case agent.CmdKillContainers:
			log.Printf("worker: KillContainers service=%s", cmd.ServiceID)
			if err := docker.KillContainersByService(cmd.ServiceID); err != nil {
				log.Printf("worker: KillContainers failed: %v", err)
				return agent.CommandResponse{Success: false, Error: err.Error()}
			}
			return agent.CommandResponse{Success: true}

		default:
			return agent.CommandResponse{Success: false, Error: "unknown command: " + cmd.Type}
		}
	})
	go cmdServer.Start()

	// Heartbeat sender
	sender := heartbeat.NewSender(
		*multicast,
		*nodeID,
		*addr,
		*interval,
		func() (float64, float64, int64, int64) {
			return agent.GetMetrics()
		},
		func() []string {
			return docker.RunningContainerIDs()
		},
	)
	go sender.Start()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("worker shutting down")
	docker.KillAllContainers()
	sender.Stop()
	cmdServer.Stop()
}
