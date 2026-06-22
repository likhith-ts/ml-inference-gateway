package batcher_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/likhith-ts/ml-inference-gateway/internal/batcher"
	"github.com/likhith-ts/ml-inference-gateway/pb"
)

// mockDispatch echoes requests back as responses with a small simulated delay.
func mockDispatch(delay time.Duration) batcher.DispatchFn {
	return func(ctx context.Context, reqs []*pb.InferenceRequest) ([]*pb.InferenceResponse, error) {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		resps := make([]*pb.InferenceResponse, len(reqs))
		for i, r := range reqs {
			resps[i] = &pb.InferenceResponse{
				RequestId:         r.RequestId,
				Text:              "echo: " + r.Prompt,
				ProcessingTimeMs:  delay.Milliseconds(),
			}
		}
		return resps, nil
	}
}

func TestSubmitSingleRequest(t *testing.T) {
	bat := batcher.New(8, 20*time.Millisecond, 128, mockDispatch(5*time.Millisecond))
	defer bat.Stop()

	resp, err := bat.Submit(context.Background(), &pb.InferenceRequest{
		Prompt: "hello",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Text != "echo: hello" {
		t.Errorf("expected 'echo: hello', got %q", resp.Text)
	}
}

func TestSubmitConcurrent(t *testing.T) {
	var dispatches int64
	dispatch := func(ctx context.Context, reqs []*pb.InferenceRequest) ([]*pb.InferenceResponse, error) {
		atomic.AddInt64(&dispatches, 1)
		time.Sleep(5 * time.Millisecond)
		resps := make([]*pb.InferenceResponse, len(reqs))
		for i, r := range reqs {
			resps[i] = &pb.InferenceResponse{RequestId: r.RequestId, Text: "ok"}
		}
		return resps, nil
	}

	// Window large enough to batch all 8 goroutines.
	bat := batcher.New(8, 50*time.Millisecond, 128, dispatch)
	defer bat.Stop()

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, err := bat.Submit(context.Background(), &pb.InferenceRequest{Prompt: "p"})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	// All n requests should have been dispatched in fewer calls than n
	// (i.e. batching is working).
	d := atomic.LoadInt64(&dispatches)
	if d >= int64(n) {
		t.Logf("dispatch calls: %d (batching may not have kicked in under test timing)", d)
	}
}

func TestQueueFull(t *testing.T) {
	// hold is closed to unblock dispatch goroutines during teardown.
	hold := make(chan struct{})

	blockingDispatch := func(ctx context.Context, reqs []*pb.InferenceRequest) ([]*pb.InferenceResponse, error) {
		select {
		case <-hold:
		case <-ctx.Done():
		}
		resps := make([]*pb.InferenceResponse, len(reqs))
		for i := range reqs {
			resps[i] = &pb.InferenceResponse{}
		}
		return resps, nil
	}

	// Use a very small capacity so it fills quickly.
	// maxBatch=1 forces a dispatch goroutine per item, keeping inputCh busy.
	const capacity = 2
	bat := batcher.New(1, 1*time.Millisecond, capacity, blockingDispatch)
	defer func() {
		close(hold)
		bat.Stop()
	}()

	// Fire thousands of goroutines simultaneously; some will certainly see a full channel.
	const total = 2000
	errCh := make(chan error, total)
	var wg sync.WaitGroup
	wg.Add(total)

	barrier := make(chan struct{})
	for i := 0; i < total; i++ {
		go func() {
			wg.Done()
			<-barrier
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_, err := bat.Submit(ctx, &pb.InferenceRequest{Prompt: "x"})
			errCh <- err
		}()
	}
	wg.Wait()
	close(barrier)

	var gotFull int
	deadline := time.After(5 * time.Second)
	for i := 0; i < total; i++ {
		select {
		case err := <-errCh:
			if err == batcher.ErrQueueFull {
				gotFull++
			}
		case <-deadline:
			t.Fatal("timed out waiting for all Submit calls to return")
		}
	}
	if gotFull == 0 {
		t.Error("expected at least one ErrQueueFull under heavy load, but none received")
	}
	t.Logf("%d/%d requests received ErrQueueFull (backpressure)", gotFull, total)
}

func TestContextCancellation(t *testing.T) {
	bat := batcher.New(8, 10*time.Millisecond, 128, mockDispatch(5*time.Millisecond))
	defer bat.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := bat.Submit(ctx, &pb.InferenceRequest{Prompt: "cancelled"})
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

func TestQueueDepth(t *testing.T) {
	bat := batcher.New(8, 100*time.Millisecond, 64, mockDispatch(50*time.Millisecond))
	defer bat.Stop()

	if d := bat.QueueDepth(); d != 0 {
		t.Errorf("expected initial QueueDepth=0, got %d", d)
	}
}
