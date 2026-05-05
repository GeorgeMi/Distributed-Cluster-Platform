package heartbeat

import (
	"encoding/json"
	"log"
	"net"
	"time"

	"github.com/GeorgeMi/Distributed-Cluster-Platform/internal/domain"
)

type Sender struct {
	multicastAddr string
	interval      time.Duration
	nodeID        string
	address       string
	getMetrics    func() (totalCPU, usedCPU float64, totalRAM, usedRAM int64)
	getContainers func() []string
	stop          chan struct{}
}

func NewSender(multicastAddr, nodeID, address string, interval time.Duration,
	getMetrics func() (float64, float64, int64, int64),
	getContainers func() []string,
) *Sender {
	return &Sender{
		multicastAddr: multicastAddr,
		interval:      interval,
		nodeID:        nodeID,
		address:       address,
		getMetrics:    getMetrics,
		getContainers: getContainers,
		stop:          make(chan struct{}),
	}
}

func (s *Sender) Start() {
	addr, err := net.ResolveUDPAddr("udp", s.multicastAddr)
	if err != nil {
		log.Fatalf("heartbeat sender: resolve addr: %v", err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("heartbeat sender: dial: %v", err)
	}
	defer conn.Close()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	log.Printf("heartbeat sender: started, sending to %s every %s", s.multicastAddr, s.interval)

	for {
		select {
		case <-ticker.C:
			s.send(conn)
		case <-s.stop:
			log.Println("heartbeat sender: stopped")
			return
		}
	}
}

func (s *Sender) Stop() {
	close(s.stop)
}

func (s *Sender) send(conn *net.UDPConn) {
	totalCPU, usedCPU, totalRAM, usedRAM := s.getMetrics()
	containers := s.getContainers()

	hb := domain.Heartbeat{
		NodeID:     s.nodeID,
		Address:    s.address,
		TotalCPU:   totalCPU,
		UsedCPU:    usedCPU,
		TotalRAM:   totalRAM,
		UsedRAM:    usedRAM,
		Containers: containers,
	}

	data, err := json.Marshal(hb)
	if err != nil {
		log.Printf("heartbeat sender: marshal: %v", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		log.Printf("heartbeat sender: write: %v", err)
	}
}
