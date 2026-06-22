// Package health provides background worker health-checking over gRPC.
package health

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/likhith-ts/ml-inference-gateway/pb"
)

// Worker holds the address and live gRPC connection to a backend node.
type Worker struct {
	Address     string
	Conn        *grpc.ClientConn
	Client      pb.InferenceWorkerClient
	ActiveConns int32 // guarded by Checker.mu
	Healthy     bool  // guarded by Checker.mu
}

// Checker manages a pool of workers and periodically probes them.
type Checker struct {
	mu       sync.RWMutex
	workers  []*Worker
	interval time.Duration
	stopCh   chan struct{}
	OnChange func(healthy []*Worker) // called whenever health state changes
}

// New creates a Checker that pings workers at the given interval.
func New(addresses []string, interval time.Duration) (*Checker, error) {
	c := &Checker{
		interval: interval,
		stopCh:   make(chan struct{}),
	}

	for _, addr := range addresses {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.workers = append(c.workers, &Worker{
			Address: addr,
			Conn:    conn,
			Client:  pb.NewInferenceWorkerClient(conn),
			Healthy: true,
		})
	}
	return c, nil
}

// Start launches the background health-probe loop.
func (c *Checker) Start() {
	go c.loop()
}

// Stop shuts down the health-probe loop.
func (c *Checker) Stop() {
	close(c.stopCh)
}

// Healthy returns a snapshot of all currently healthy workers.
func (c *Checker) Healthy() []*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Worker, 0, len(c.workers))
	for _, w := range c.workers {
		if w.Healthy {
			out = append(out, w)
		}
	}
	return out
}

// All returns all registered workers regardless of health.
func (c *Checker) All() []*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Worker, len(c.workers))
	copy(out, c.workers)
	return out
}

func (c *Checker) loop() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	// Run an initial probe immediately.
	c.probe()
	for {
		select {
		case <-ticker.C:
			c.probe()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Checker) probe() {
	changed := false
	for _, w := range c.workers {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := w.Client.CheckHealth(ctx, &pb.HealthRequest{})
		cancel()

		c.mu.Lock()
		wasHealthy := w.Healthy
		if err != nil || resp.GetStatus() != pb.HealthResponse_HEALTHY {
			w.Healthy = false
		} else {
			w.Healthy = true
		}
		if wasHealthy != w.Healthy {
			changed = true
		}
		c.mu.Unlock()
	}

	if changed && c.OnChange != nil {
		c.OnChange(c.Healthy())
	}
}
