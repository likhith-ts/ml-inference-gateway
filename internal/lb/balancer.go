// Package lb implements a least-connections load balancer over healthy workers.
package lb

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/likhith-ts/ml-inference-gateway/internal/health"
)

// ErrNoWorkers is returned when no healthy workers are available.
var ErrNoWorkers = errors.New("lb: no healthy workers available")

// Balancer selects the worker with the fewest active connections.
type Balancer struct {
	mu      sync.RWMutex
	workers []*health.Worker
}

// New creates a Balancer with an initial set of workers.
func New(workers []*health.Worker) *Balancer {
	return &Balancer{workers: workers}
}

// Update replaces the live worker list (called when health changes).
func (b *Balancer) Update(workers []*health.Worker) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.workers = workers
}

// Pick returns the worker with the minimum active connections.
// It atomically increments that worker's ActiveConns counter so the caller
// must call Done() when the request completes.
func (b *Balancer) Pick() (*health.Worker, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.workers) == 0 {
		return nil, ErrNoWorkers
	}

	var best *health.Worker
	var bestVal int32

	for _, w := range b.workers {
		v := atomic.LoadInt32(&w.ActiveConns)
		if best == nil || v < bestVal {
			best = w
			bestVal = v
		}
	}

	if best == nil {
		return nil, ErrNoWorkers
	}

	atomic.AddInt32(&best.ActiveConns, 1)
	return best, nil
}

// Done decrements the active connection counter for a worker after a request
// finishes. It is safe to call Done even after the worker has been removed
// from the pool.
func Done(w *health.Worker) {
	atomic.AddInt32(&w.ActiveConns, -1)
}
