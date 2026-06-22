package lb_test

import (
	"sync/atomic"
	"testing"

	"github.com/likhith-ts/ml-inference-gateway/internal/health"
	"github.com/likhith-ts/ml-inference-gateway/internal/lb"
)

func makeWorker(addr string) *health.Worker {
	return &health.Worker{
		Address: addr,
		Healthy: true,
	}
}

func TestPickLeastConnections(t *testing.T) {
	w1 := makeWorker("worker1:50052")
	w2 := makeWorker("worker2:50052")
	w3 := makeWorker("worker3:50052")

	// Simulate w1 having 5 active connections, w2 having 2, w3 having 0.
	atomic.StoreInt32(&w1.ActiveConns, 5)
	atomic.StoreInt32(&w2.ActiveConns, 2)
	atomic.StoreInt32(&w3.ActiveConns, 0)

	b := lb.New([]*health.Worker{w1, w2, w3})

	picked, err := b.Pick()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if picked.Address != w3.Address {
		t.Errorf("expected w3 (0 conns) to be picked, got %s", picked.Address)
	}
	// Pick should have incremented the counter.
	if atomic.LoadInt32(&w3.ActiveConns) != 1 {
		t.Errorf("expected w3.ActiveConns=1, got %d", atomic.LoadInt32(&w3.ActiveConns))
	}
}

func TestPickNoWorkers(t *testing.T) {
	b := lb.New([]*health.Worker{})
	_, err := b.Pick()
	if err != lb.ErrNoWorkers {
		t.Errorf("expected ErrNoWorkers, got %v", err)
	}
}

func TestDone(t *testing.T) {
	w := makeWorker("worker1:50052")
	b := lb.New([]*health.Worker{w})

	picked, err := b.Pick()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&picked.ActiveConns) != 1 {
		t.Errorf("expected 1 active conn after Pick, got %d", atomic.LoadInt32(&picked.ActiveConns))
	}
	lb.Done(picked)
	if atomic.LoadInt32(&picked.ActiveConns) != 0 {
		t.Errorf("expected 0 active conns after Done, got %d", atomic.LoadInt32(&picked.ActiveConns))
	}
}

func TestUpdate(t *testing.T) {
	w1 := makeWorker("worker1:50052")
	b := lb.New([]*health.Worker{w1})

	// Replace with an empty pool.
	b.Update([]*health.Worker{})
	_, err := b.Pick()
	if err != lb.ErrNoWorkers {
		t.Errorf("expected ErrNoWorkers after Update([]), got %v", err)
	}

	// Re-add a worker.
	w2 := makeWorker("worker2:50052")
	b.Update([]*health.Worker{w2})
	picked, err := b.Pick()
	if err != nil {
		t.Fatalf("unexpected error after re-adding worker: %v", err)
	}
	if picked.Address != w2.Address {
		t.Errorf("expected w2 to be picked, got %s", picked.Address)
	}
}

func TestConcurrentPick(t *testing.T) {
	workers := make([]*health.Worker, 3)
	for i := range workers {
		workers[i] = makeWorker("worker")
	}
	b := lb.New(workers)

	const goroutines = 100
	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			w, err := b.Pick()
			if err == nil {
				lb.Done(w)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	// All counters should be back to 0.
	for _, w := range workers {
		if v := atomic.LoadInt32(&w.ActiveConns); v != 0 {
			t.Errorf("expected 0 active conns, got %d", v)
		}
	}
}
