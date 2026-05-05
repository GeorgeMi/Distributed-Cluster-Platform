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
	addr := flag.String("addr", "localhost:7000", "node address")
	multicast := flag.String("multicast", "239.0.0.1:9090", "multicast address for heartbeats")
	interval := flag.Duration("interval", 5*time.Second, "heartbeat interval")
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "usage: worker -id <node-id> [-addr host:port] [-multicast addr:port]")
		os.Exit(1)
	}

	log.Printf("worker %s starting, address=%s, multicast=%s", *nodeID, *addr, *multicast)

	sender := heartbeat.NewSender(
		*multicast,
		*nodeID,
		*addr,
		*interval,
		func() (float64, float64, int64, int64) {
			return agent.GetMetrics()
		},
		func() []string {
			return nil // no containers yet
		},
	)

	go sender.Start()

	// Wait for SIGINT/SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("worker shutting down")
	sender.Stop()
}
