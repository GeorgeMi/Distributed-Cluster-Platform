package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/heartbeat"
)

func main() {
	multicast := flag.String("multicast", "239.0.0.1:9090", "multicast address for heartbeats")
	flag.Parse()

	log.Printf("master starting, listening for heartbeats on %s", *multicast)

	state := cluster.NewState()

	// Add default resource pools
	state.AddPool(&domain.ResourcePool{ID: "high-cpu", Name: "high-cpu", MinCPU: 4, MaxCPU: 128, MinRAM: 0, MaxRAM: 32768})
	state.AddPool(&domain.ResourcePool{ID: "high-ram", Name: "high-ram", MinCPU: 0, MaxCPU: 128, MinRAM: 32769, MaxRAM: 1048576})

	receiver := heartbeat.NewReceiver(*multicast, state)
	go receiver.Start()

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

	// Wait for SIGINT/SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("master shutting down")
	receiver.Stop()
}
