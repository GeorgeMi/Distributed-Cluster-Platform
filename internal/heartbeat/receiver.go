package heartbeat

import (
	"encoding/json"
	"log"
	"net"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/cluster"
	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

type Receiver struct {
	multicastAddr    string
	state            *cluster.State
	onLateHeartbeat  func(nodeID string)
	onNodeDiscovered func(nodeID string)
	stop             chan struct{}
}

func NewReceiver(multicastAddr string, state *cluster.State, onLateHeartbeat func(nodeID string), onNodeDiscovered func(nodeID string)) *Receiver {
	return &Receiver{
		multicastAddr:    multicastAddr,
		state:            state,
		onLateHeartbeat:  onLateHeartbeat,
		onNodeDiscovered: onNodeDiscovered,
		stop:             make(chan struct{}),
	}
}

func (r *Receiver) Start() {
	addr, err := net.ResolveUDPAddr("udp", r.multicastAddr)
	if err != nil {
		log.Fatalf("heartbeat receiver: resolve addr: %v", err)
	}

	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("heartbeat receiver: listen: %v", err)
	}
	defer conn.Close()

	conn.SetReadBuffer(8192)
	log.Printf("heartbeat receiver: listening on %s", r.multicastAddr)

	buf := make([]byte, 4096)
	for {
		select {
		case <-r.stop:
			log.Println("heartbeat receiver: stopped")
			return
		default:
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				log.Printf("heartbeat receiver: read: %v", err)
				continue
			}
			r.handleHeartbeat(buf[:n])
		}
	}
}

func (r *Receiver) Stop() {
	close(r.stop)
}

func (r *Receiver) handleHeartbeat(data []byte) {
	var hb domain.Heartbeat
	if err := json.Unmarshal(data, &hb); err != nil {
		log.Printf("heartbeat receiver: unmarshal: %v", err)
		return
	}

	node, exists := r.state.GetNode(hb.NodeID)
	if !exists {
		newNode := &domain.Node{
			ID:       hb.NodeID,
			Address:  hb.Address,
			Status:   domain.NodeAlive,
			TotalCPU: hb.TotalCPU,
			UsedCPU:  hb.UsedCPU,
			TotalRAM: hb.TotalRAM,
			UsedRAM:  hb.UsedRAM,
		}
		r.state.AddNode(newNode)
		r.state.AssignNodeToPool(hb.NodeID, newNode)
		log.Printf("heartbeat receiver: new node discovered: %s (%s) CPU=%.1f RAM=%dMB",
			hb.NodeID, hb.Address, hb.TotalCPU, hb.TotalRAM)
		if r.onNodeDiscovered != nil {
			r.onNodeDiscovered(hb.NodeID)
		}
	} else if node.Status == domain.NodeDead && r.onLateHeartbeat != nil {
		r.onLateHeartbeat(hb.NodeID)
	}

	// Update metrics for every heartbeat
	r.state.UpdateNodeMetrics(hb.NodeID, hb.UsedCPU, hb.UsedRAM, hb.ActiveConnections)
}
