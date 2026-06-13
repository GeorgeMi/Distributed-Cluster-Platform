package agent

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// DockerManager handles starting and stopping containers via the Docker SDK.
// Containers are tagged with Docker labels (node ID and service ID), so the
// worker can find its own containers even after a crash and restart.
type DockerManager struct {
	cli    *client.Client
	nodeID string
	mu     sync.Mutex
	seq    int
}

func NewDockerManager(nodeID string) (*DockerManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &DockerManager{
		cli:    cli,
		nodeID: nodeID,
	}, nil
}

// StartContainer pulls the image (if needed) and starts a container.
func (dm *DockerManager) StartContainer(cmd Command) (string, error) {
	ctx := context.Background()

	// Pull the image only if it is not already present
	if _, err := dm.cli.ImageInspect(ctx, cmd.Image); err != nil {
		log.Printf("docker: pulling image %s", cmd.Image)
		reader, err := dm.cli.ImagePull(ctx, cmd.Image, image.PullOptions{})
		if err != nil {
			return "", fmt.Errorf("pull image %s: %w", cmd.Image, err)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	var env []string
	for k, v := range cmd.EnvVars {
		env = append(env, k+"="+v)
	}

	exposedPorts := nat.PortSet{}
	portBindings := nat.PortMap{}
	for _, p := range cmd.Ports {
		port := nat.Port(strconv.Itoa(p) + "/tcp")
		exposedPorts[port] = struct{}{}
		portBindings[port] = []nat.PortBinding{
			{HostIP: "0.0.0.0", HostPort: strconv.Itoa(p)},
		}
	}

	containerConfig := &container.Config{
		Image:        cmd.Image,
		Env:          env,
		Cmd:          cmd.Cmd,
		ExposedPorts: exposedPorts,
		Labels: map[string]string{
			"dcp.node":    dm.nodeID,
			"dcp.service": cmd.ServiceID,
		},
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		Resources: container.Resources{
			NanoCPUs: int64(cmd.CPULimit * 1e9),
			Memory:   cmd.RAMLimit * 1024 * 1024, // MB to bytes
		},
	}

	// The node ID in the name keeps it unique when workers share one Docker daemon.
	dm.mu.Lock()
	dm.seq++
	containerName := fmt.Sprintf("dcp-%s-%s-%d", dm.nodeID, cmd.ServiceID, dm.seq)
	dm.mu.Unlock()
	resp, err := dm.cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", fmt.Errorf("create container: %w", err)
	}

	if err := dm.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		// Remove the created container on failure so it does not leak.
		dm.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("start container: %w", err)
	}

	log.Printf("docker: started container %s (image=%s, name=%s)", resp.ID[:12], cmd.Image, containerName)
	return resp.ID, nil
}

// KillContainersByService stops and removes this node's containers for a
// specific service. If serviceID is empty, all of this node's containers
// are removed.
func (dm *DockerManager) KillContainersByService(serviceID string) error {
	toKill, err := dm.listOwnContainers(serviceID)
	if err != nil {
		return err
	}
	dm.removeContainers(toKill)
	log.Printf("docker: killed %d container(s) for service=%s", len(toKill), serviceID)
	return nil
}

// KillAllContainers stops and removes all containers managed by this worker.
func (dm *DockerManager) KillAllContainers() error {
	all, err := dm.listOwnContainers("")
	if err != nil {
		return err
	}
	dm.removeContainers(all)
	log.Printf("docker: killed %d container(s)", len(all))
	return nil
}

// RunningContainerIDs returns the IDs of running containers owned by this worker.
func (dm *DockerManager) RunningContainerIDs() []string {
	ctx := context.Background()
	list, err := dm.cli.ContainerList(ctx, container.ListOptions{
		Filters: dm.ownFilter(""),
	})
	if err != nil {
		log.Printf("docker: list containers: %v", err)
		return nil
	}
	var ids []string
	for _, c := range list {
		ids = append(ids, c.ID)
	}
	return ids
}

func (dm *DockerManager) ownFilter(serviceID string) filters.Args {
	args := filters.NewArgs(filters.Arg("label", "dcp.node="+dm.nodeID))
	if serviceID != "" {
		args.Add("label", "dcp.service="+serviceID)
	}
	return args
}

func (dm *DockerManager) listOwnContainers(serviceID string) ([]string, error) {
	ctx := context.Background()
	list, err := dm.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: dm.ownFilter(serviceID),
	})
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	var ids []string
	for _, c := range list {
		ids = append(ids, c.ID)
	}
	return ids, nil
}

func (dm *DockerManager) removeContainers(ids []string) {
	ctx := context.Background()
	for _, containerID := range ids {
		log.Printf("docker: stopping container %s", containerID[:12])
		if err := dm.cli.ContainerStop(ctx, containerID, container.StopOptions{}); err != nil {
			log.Printf("docker: stop %s: %v", containerID[:12], err)
		}
		if err := dm.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{}); err != nil {
			log.Printf("docker: remove %s: %v", containerID[:12], err)
		}
	}
}
