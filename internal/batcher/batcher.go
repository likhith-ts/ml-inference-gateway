// Package batcher implements a thread-safe dynamic request batching engine.
//
// Individual client requests arrive on Submit(); the batcher accumulates them
// for up to WindowDuration or until MaxBatchSize requests are collected,
// then dispatches the whole batch to a backend worker in a single RPC.
package batcher

import (
	"context"
	"sync"
	"time"

	"github.com/likhith-ts/ml-inference-gateway/pb"
)

// pendingRequest holds a single client's request and the channel on which the
// response (or error) will be delivered.
type pendingRequest struct {
	req    *pb.InferenceRequest
	respCh chan result
}

// result wraps either a successful response or an error.
type result struct {
	resp *pb.InferenceResponse
	err  error
}

// DispatchFn is the function called by the batcher to send a batch to a worker.
// Implementations should forward the batch via gRPC and return responses in
// the same order as the input requests.
type DispatchFn func(ctx context.Context, reqs []*pb.InferenceRequest) ([]*pb.InferenceResponse, error)

// Batcher accumulates requests and dispatches them in batches.
type Batcher struct {
	maxBatch int
	window   time.Duration
	dispatch DispatchFn
	capacity int

	mu      sync.Mutex
	pending []pendingRequest

	inputCh  chan pendingRequest
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// New creates and starts a Batcher.
//
//   - maxBatch:  maximum requests per batch dispatch
//   - window:    maximum time to wait before flushing a partial batch
//   - capacity:  maximum number of requests that may queue (backpressure)
//   - dispatch:  function used to send batches to workers
func New(maxBatch int, window time.Duration, capacity int, dispatch DispatchFn) *Batcher {
	b := &Batcher{
		maxBatch: maxBatch,
		window:   window,
		dispatch: dispatch,
		capacity: capacity,
		inputCh:  make(chan pendingRequest, capacity),
		stopCh:   make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// Submit enqueues a single inference request and blocks until the batch
// containing it has been dispatched and a response is available.
// Returns context.Canceled / DeadlineExceeded if the caller's context expires.
// Returns ErrQueueFull when the queue capacity is exhausted (backpressure).
func (b *Batcher) Submit(ctx context.Context, req *pb.InferenceRequest) (*pb.InferenceResponse, error) {
	pr := pendingRequest{
		req:    req,
		respCh: make(chan result, 1),
	}

	select {
	case b.inputCh <- pr:
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, ErrQueueFull
	}

	select {
	case r := <-pr.respCh:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Stop shuts down the batcher's dispatch goroutine gracefully.
func (b *Batcher) Stop() {
	close(b.stopCh)
	b.wg.Wait()
}

// QueueDepth returns the current number of requests waiting to be batched.
func (b *Batcher) QueueDepth() int {
	return len(b.inputCh)
}

func (b *Batcher) run() {
	defer b.wg.Done()

	timer := time.NewTimer(b.window)
	defer timer.Stop()

	batch := make([]pendingRequest, 0, b.maxBatch)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		go b.dispatchBatch(batch)
		batch = make([]pendingRequest, 0, b.maxBatch)
	}

	for {
		select {
		case pr := <-b.inputCh:
			batch = append(batch, pr)
			if len(batch) >= b.maxBatch {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				flush()
				timer.Reset(b.window)
			}

		case <-timer.C:
			flush()
			timer.Reset(b.window)

		case <-b.stopCh:
			// Drain any remaining requests.
			for {
				select {
				case pr := <-b.inputCh:
					batch = append(batch, pr)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (b *Batcher) dispatchBatch(batch []pendingRequest) {
	reqs := make([]*pb.InferenceRequest, len(batch))
	for i, pr := range batch {
		reqs[i] = pr.req
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resps, err := b.dispatch(ctx, reqs)

	for i, pr := range batch {
		var r result
		if err != nil {
			r = result{err: err}
		} else if i < len(resps) {
			r = result{resp: resps[i]}
		} else {
			r = result{err: ErrMissingResponse}
		}
		pr.respCh <- r
	}
}
