package agent

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// DockerManager handles starting and stopping containers via the Docker SDK.
type DockerManager struct {
	cli        *client.Client
	mu         sync.Mutex
	containers map[string]string // serviceID -> dockerContainerID
}

func NewDockerManager() (*DockerManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &DockerManager{
		cli:        cli,
		containers: make(map[string]string),
	}, nil
}

// StartContainer pulls the image (if needed) and starts a container.
func (dm *DockerManager) StartContainer(cmd Command) (string, error) {
	ctx := context.Background()

	// Pull image
	log.Printf("docker: pulling image %s", cmd.Image)
	reader, err := dm.cli.ImagePull(ctx, cmd.Image, image.PullOptions{})
	if err != nil {
		return "", fmt.Errorf("pull image %s: %w", cmd.Image, err)
	}
	io.Copy(io.Discard, reader)
	reader.Close()

	// Build environment variables
	var env []string
	for k, v := range cmd.EnvVars {
		env = append(env, k+"="+v)
	}

	// Build port bindings
	exposedPorts := nat.PortSet{}
	portBindings := nat.PortMap{}
	for _, p := range cmd.Ports {
		port := nat.Port(strconv.Itoa(p) + "/tcp")
		exposedPorts[port] = struct{}{}
		portBindings[port] = []nat.PortBinding{
			{HostIP: "0.0.0.0", HostPort: strconv.Itoa(p)},
		}
	}

	// Container config
	containerConfig := &container.Config{
		Image:        cmd.Image,
		Env:          env,
		Cmd:          cmd.Cmd,
		ExposedPorts: exposedPorts,
	}

	// Host config with resource limits
	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		Resources: container.Resources{
			NanoCPUs: int64(cmd.CPULimit * 1e9),
			Memory:   cmd.RAMLimit * 1024 * 1024, // MB to bytes
		},
	}

	// Create container
	containerName := fmt.Sprintf("dcp-%s", cmd.ServiceID)
	resp, err := dm.cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", fmt.Errorf("create container: %w", err)
	}

	// Start container
	if err := dm.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start container: %w", err)
	}

	dm.mu.Lock()
	dm.containers[cmd.ServiceID] = resp.ID
	dm.mu.Unlock()

	log.Printf("docker: started container %s (image=%s, name=%s)", resp.ID[:12], cmd.Image, containerName)
	return resp.ID, nil
}

// KillContainersByService stops and removes containers for a specific service.
// If serviceID is empty, kills all containers.
func (dm *DockerManager) KillContainersByService(serviceID string) error {
	ctx := context.Background()

	dm.mu.Lock()
	var toKill []string
	for svcID, containerID := range dm.containers {
		if serviceID == "" || svcID == serviceID {
			toKill = append(toKill, containerID)
			delete(dm.containers, svcID)
		}
	}
	dm.mu.Unlock()

	for _, containerID := range toKill {
		log.Printf("docker: stopping container %s", containerID[:12])
		if err := dm.cli.ContainerStop(ctx, containerID, container.StopOptions{}); err != nil {
			log.Printf("docker: stop %s: %v", containerID[:12], err)
		}
		if err := dm.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{}); err != nil {
			log.Printf("docker: remove %s: %v", containerID[:12], err)
		}
	}

	log.Printf("docker: killed %d container(s) for service=%s", len(toKill), serviceID)
	return nil
}

// KillAllContainers stops and removes all containers managed by this worker.
func (dm *DockerManager) KillAllContainers() error {
	ctx := context.Background()

	dm.mu.Lock()
	ids := make(map[string]string)
	for k, v := range dm.containers {
		ids[k] = v
	}
	dm.mu.Unlock()

	for svcID, containerID := range ids {
		log.Printf("docker: stopping container %s (service=%s)", containerID[:12], svcID)
		if err := dm.cli.ContainerStop(ctx, containerID, container.StopOptions{}); err != nil {
			log.Printf("docker: stop %s: %v", containerID[:12], err)
		}
		if err := dm.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{}); err != nil {
			log.Printf("docker: remove %s: %v", containerID[:12], err)
		}
	}

	dm.mu.Lock()
	dm.containers = make(map[string]string)
	dm.mu.Unlock()

	log.Printf("docker: killed %d container(s)", len(ids))
	return nil
}

// RunningContainerIDs returns the IDs of containers managed by this worker.
func (dm *DockerManager) RunningContainerIDs() []string {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	var ids []string
	for _, id := range dm.containers {
		ids = append(ids, id)
	}
	return ids
}
