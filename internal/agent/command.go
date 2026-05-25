package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

// Command types sent from master to worker
const (
	CmdStartContainer = "StartContainer"
	CmdKillContainers = "KillContainers"
)

// Command is a message from master to worker.
type Command struct {
	Type        string            `json:"type"`
	ContainerID string            `json:"containerID,omitempty"`
	Image       string            `json:"image,omitempty"`
	CPULimit    float64           `json:"cpuLimit,omitempty"`
	RAMLimit    int64             `json:"ramLimit,omitempty"`
	EnvVars     map[string]string `json:"envVars,omitempty"`
	Ports       []int             `json:"ports,omitempty"`
	Cmd         []string          `json:"cmd,omitempty"`
	ServiceID   string            `json:"serviceID,omitempty"`
}

// CommandResponse is the worker's reply to a command.
type CommandResponse struct {
	Success     bool   `json:"success"`
	ContainerID string `json:"containerID,omitempty"`
	Error       string `json:"error,omitempty"`
}

// CommandHandler processes commands received by the worker.
type CommandHandler func(cmd Command) CommandResponse

// CommandServer listens for TCP commands from the master.
type CommandServer struct {
	listenAddr string
	handler    CommandHandler
	stop       chan struct{}
}

func NewCommandServer(listenAddr string, handler CommandHandler) *CommandServer {
	return &CommandServer{
		listenAddr: listenAddr,
		handler:    handler,
		stop:       make(chan struct{}),
	}
}

func (s *CommandServer) Start() {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		log.Fatalf("command server: listen: %v", err)
	}
	defer ln.Close()

	log.Printf("command server: listening on %s", s.listenAddr)

	go func() {
		<-s.stop
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				log.Printf("command server: accept: %v", err)
				continue
			}
		}
		go s.handleConn(conn)
	}
}

func (s *CommandServer) Stop() {
	close(s.stop)
}

func (s *CommandServer) handleConn(conn net.Conn) {
	defer conn.Close()

	var cmd Command
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&cmd); err != nil {
		log.Printf("command server: decode: %v", err)
		return
	}

	log.Printf("command server: received %s", cmd.Type)

	resp := s.handler(cmd)

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(resp); err != nil {
		log.Printf("command server: encode response: %v", err)
	}
}

// SendCommand sends a command from master to a worker node via TCP.
func SendCommand(nodeAddr string, cmd Command) (CommandResponse, error) {
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return CommandResponse{}, fmt.Errorf("connect to %s: %w", nodeAddr, err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(cmd); err != nil {
		return CommandResponse{}, fmt.Errorf("send command: %w", err)
	}

	var resp CommandResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&resp); err != nil {
		return CommandResponse{}, fmt.Errorf("read response: %w", err)
	}

	return resp, nil
}
