#!/usr/bin/env python3
"""
Mock ML inference worker.

Simulates realistic LLM inference latency without real GPU hardware.
Each request is "processed" with a configurable base delay plus a
per-token cost, making batched requests faster per-token than individual
ones (simulating real hardware utilisation gains).

Usage:
    python mock_worker.py --port 50052 --worker-id worker-1
"""

import argparse
import logging
import math
import random
import sys
import time
from concurrent import futures

import grpc

# The generated proto stubs are vendored into the workers/ directory.
# If running outside Docker, ensure the pb/ package is on PYTHONPATH or
# install via:  pip install grpcio grpcio-tools
sys.path.insert(0, "/app")

try:
    from pb import inference_pb2, inference_pb2_grpc
except ImportError:
    # Fallback: generate stubs on the fly if protoc is available.
    print(
        "ERROR: Could not import pb. Please run 'make proto' or mount the pb/ directory.",
        file=sys.stderr,
    )
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("mock_worker")

# ── Latency simulation constants ─────────────────────────────────────────────

BASE_DELAY_MS = 50       # Fixed overhead per batch (GPU initialisation, etc.)
PER_TOKEN_MS  = 8        # Milliseconds simulated per requested token
BATCH_SPEEDUP = 0.65     # Batching reduces per-token cost to ~65 % of serial


def simulate_inference(requests) -> list:
    """
    Simulate ML inference for a list of InferenceRequest proto messages.

    Applies a realistic latency model:
        total_ms = BASE_DELAY_MS + (sum_tokens * PER_TOKEN_MS * batch_efficiency)

    Returns a list of (text, processing_time_ms) tuples.
    """
    n = len(requests)
    total_tokens = sum(max(r.max_tokens, 1) for r in requests)

    # Batch efficiency: larger batches process tokens faster per request.
    if n > 1:
        efficiency = BATCH_SPEEDUP / math.log2(n + 1)
    else:
        efficiency = 1.0

    delay_ms = BASE_DELAY_MS + total_tokens * PER_TOKEN_MS * efficiency
    # Add ±10 % jitter to simulate real hardware variance.
    jitter = random.uniform(0.9, 1.1)
    delay_ms *= jitter

    time.sleep(delay_ms / 1000.0)

    results = []
    for req in requests:
        # Generate a plausible-looking token sequence.
        n_tokens = max(req.max_tokens, 1)
        tokens = [f"[tok_{i}]" for i in range(min(n_tokens, 20))]
        text = "Simulated response: " + " ".join(tokens)
        results.append((text, int(delay_ms)))

    return results


# ── gRPC service implementation ───────────────────────────────────────────────

class InferenceWorkerServicer(inference_pb2_grpc.InferenceWorkerServicer):

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self._active_load: int = 0

    def Predict(self, request, context):
        batch = list(request.requests)
        n = len(batch)
        self._active_load += n
        log.info("[%s] Received batch of %d request(s)", self.worker_id, n)

        try:
            results = simulate_inference(batch)
        except Exception as exc:  # pragma: no cover
            log.exception("[%s] Inference failed: %s", self.worker_id, exc)
            context.abort(grpc.StatusCode.INTERNAL, str(exc))
            return
        finally:
            self._active_load -= n

        responses = [
            inference_pb2.InferenceResponse(
                request_id=req.request_id,
                text=text,
                processing_time_ms=ms,
            )
            for req, (text, ms) in zip(batch, results)
        ]
        log.info(
            "[%s] Completed batch of %d request(s) in %d ms",
            self.worker_id,
            n,
            results[0][1] if results else 0,
        )
        return inference_pb2.BatchInferenceResponse(responses=responses)

    def CheckHealth(self, request, context):
        return inference_pb2.HealthResponse(
            status=inference_pb2.HealthResponse.HEALTHY,
            active_load=self._active_load,
        )


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Mock ML inference worker")
    parser.add_argument("--port", type=int, default=50052, help="gRPC port to listen on")
    parser.add_argument("--worker-id", default="worker-1", help="Human-readable worker identifier")
    parser.add_argument("--max-workers", type=int, default=10, help="gRPC thread pool size")
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.max_workers))
    inference_pb2_grpc.add_InferenceWorkerServicer_to_server(
        InferenceWorkerServicer(args.worker_id), server
    )
    listen_addr = f"[::]:{args.port}"
    server.add_insecure_port(listen_addr)
    server.start()
    log.info("[%s] Worker listening on %s", args.worker_id, listen_addr)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("[%s] Shutting down...", args.worker_id)
        server.stop(grace=5)


if __name__ == "__main__":
    main()
