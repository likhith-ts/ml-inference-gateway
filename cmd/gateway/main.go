// Package main is the entry point for the ML inference gateway.
//
// The gateway exposes:
//   - A gRPC endpoint on :50051 (InferenceGateway service)
//   - An HTTP/REST endpoint on :8080 (POST /v1/chat/completions)
//   - A Prometheus metrics endpoint on :2112/metrics
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/likhith-ts/ml-inference-gateway/internal/batcher"
	"github.com/likhith-ts/ml-inference-gateway/internal/health"
	"github.com/likhith-ts/ml-inference-gateway/internal/lb"
	"github.com/likhith-ts/ml-inference-gateway/internal/metrics"
	"github.com/likhith-ts/ml-inference-gateway/pb"
)

// ─── CLI flags ───────────────────────────────────────────────────────────────

var (
	grpcAddr    = flag.String("grpc-addr", ":50051", "Gateway gRPC listen address")
	httpAddr    = flag.String("http-addr", ":8080", "Gateway HTTP listen address")
	metricsAddr = flag.String("metrics-addr", ":2112", "Prometheus metrics listen address")
	workers     = flag.String("workers", "localhost:50052,localhost:50053,localhost:50054",
		"Comma-separated list of backend worker gRPC addresses")
	healthInterval = flag.Duration("health-interval", 5*time.Second, "Worker health check interval")
	batchWindow    = flag.Duration("batch-window", 10*time.Millisecond, "Dynamic batching time window")
	maxBatch       = flag.Int("max-batch", 8, "Maximum requests per batch")
	queueCapacity  = flag.Int("queue-capacity", 512, "Maximum queued requests (backpressure limit)")
)

// ─── gRPC gateway service implementation ─────────────────────────────────────

// gatewayServer implements the pb.InferenceGatewayServer interface.
type gatewayServer struct {
	pb.UnimplementedInferenceWorkerServer
	bat *batcher.Batcher
}

func (g *gatewayServer) Predict(ctx context.Context, req *pb.BatchInferenceRequest) (*pb.BatchInferenceResponse, error) {
	start := time.Now()

	// Fan out each request in the batch to the dynamic batcher individually
	// (the batcher will re-group them with other concurrent requests).
	type pair struct {
		idx  int
		resp *pb.InferenceResponse
		err  error
	}

	ch := make(chan pair, len(req.Requests))
	for i, r := range req.Requests {
		go func(idx int, r *pb.InferenceRequest) {
			resp, err := g.bat.Submit(ctx, r)
			ch <- pair{idx, resp, err}
		}(i, r)
	}

	resps := make([]*pb.InferenceResponse, len(req.Requests))
	for range req.Requests {
		p := <-ch
		if p.err != nil {
			metrics.RequestsTotal.WithLabelValues("error").Inc()
			if p.err == batcher.ErrQueueFull {
				return nil, status.Error(codes.ResourceExhausted, p.err.Error())
			}
			return nil, status.Error(codes.Internal, p.err.Error())
		}
		resps[p.idx] = p.resp
	}

	metrics.RequestDuration.Observe(time.Since(start).Seconds())
	metrics.RequestsTotal.WithLabelValues("ok").Add(float64(len(resps)))

	return &pb.BatchInferenceResponse{Responses: resps}, nil
}

func (g *gatewayServer) CheckHealth(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: pb.HealthResponse_HEALTHY}, nil
}

// ─── HTTP handler ─────────────────────────────────────────────────────────────

// chatRequest mirrors the OpenAI /v1/chat/completions request body.
type chatRequest struct {
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	MaxTokens int32  `json:"max_tokens"`
}

// chatResponse is the simplified response returned over HTTP.
type chatResponse struct {
	Text             string `json:"text"`
	ProcessingTimeMs int64  `json:"processing_time_ms"`
}

func httpHandler(bat *batcher.Batcher) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		var req chatRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		start := time.Now()
		inferReq := &pb.InferenceRequest{
			Prompt:    req.Prompt,
			MaxTokens: req.MaxTokens,
		}

		resp, err := bat.Submit(r.Context(), inferReq)
		if err != nil {
			if err == batcher.ErrQueueFull {
				http.Error(w, "too many requests", http.StatusTooManyRequests)
				metrics.RequestsTotal.WithLabelValues("rate_limited").Inc()
				return
			}
			http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
			metrics.RequestsTotal.WithLabelValues("error").Inc()
			return
		}

		metrics.RequestDuration.Observe(time.Since(start).Seconds())
		metrics.RequestsTotal.WithLabelValues("ok").Inc()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(chatResponse{
			Text:             resp.GetText(),
			ProcessingTimeMs: resp.GetProcessingTimeMs(),
		})
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	return mux
}

// ─── Dispatch function (batcher → worker via LB) ──────────────────────────────

func makeDispatch(balancer *lb.Balancer) batcher.DispatchFn {
	return func(ctx context.Context, reqs []*pb.InferenceRequest) ([]*pb.InferenceResponse, error) {
		worker, err := balancer.Pick()
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		defer lb.Done(worker)

		metrics.WorkerConnections.WithLabelValues(worker.Address).Inc()
		defer metrics.WorkerConnections.WithLabelValues(worker.Address).Dec()

		metrics.BatchSize.Observe(float64(len(reqs)))

		resp, err := worker.Client.Predict(ctx, &pb.BatchInferenceRequest{Requests: reqs})
		if err != nil {
			return nil, err
		}
		return resp.Responses, nil
	}
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	workerAddrs := strings.Split(*workers, ",")
	for i, a := range workerAddrs {
		workerAddrs[i] = strings.TrimSpace(a)
	}

	// 1. Start worker health checker.
	checker, err := health.New(workerAddrs, *healthInterval)
	if err != nil {
		log.Fatalf("failed to create health checker: %v", err)
	}

	// 2. Build load balancer; wire health changes to update the pool.
	balancer := lb.New(checker.All())
	checker.OnChange = func(healthy []*health.Worker) {
		balancer.Update(healthy)
		metrics.ActiveWorkers.Set(float64(len(healthy)))
		log.Printf("worker pool updated: %d healthy node(s)", len(healthy))
	}
	checker.Start()
	defer checker.Stop()

	// Initial pool population after first health probe.
	metrics.ActiveWorkers.Set(float64(len(checker.All())))

	// 3. Start the dynamic batcher.
	bat := batcher.New(*maxBatch, *batchWindow, *queueCapacity, makeDispatch(balancer))
	defer bat.Stop()

	// Export queue depth to Prometheus on a ticker.
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			metrics.QueueDepth.Set(float64(bat.QueueDepth()))
		}
	}()

	// 4. Start gRPC gateway server.
	grpcLis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen gRPC on %s: %v", *grpcAddr, err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterInferenceWorkerServer(grpcSrv, &gatewayServer{bat: bat})

	go func() {
		log.Printf("gRPC gateway listening on %s", *grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// 5. Start HTTP server.
	httpSrv := &http.Server{
		Addr:         *httpAddr,
		Handler:      httpHandler(bat),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	go func() {
		log.Printf("HTTP gateway listening on %s", *httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// 6. Start Prometheus metrics server.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsSrv := &http.Server{
		Addr:    *metricsAddr,
		Handler: metricsMux,
	}
	go func() {
		log.Printf("Prometheus metrics listening on %s/metrics", *metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	fmt.Fprintf(os.Stdout, `
ML Inference Gateway started
  gRPC  : %s
  HTTP  : %s
  Metrics: %s/metrics
`, *grpcAddr, *httpAddr, *metricsAddr)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down...")

	grpcSrv.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)    //nolint:errcheck
	metricsSrv.Shutdown(ctx) //nolint:errcheck

	log.Println("gateway stopped")
}
