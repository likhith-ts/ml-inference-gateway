package batcher

import "errors"

// ErrQueueFull is returned by Submit when the input channel is at capacity.
// Callers should return HTTP 429 / gRPC RESOURCE_EXHAUSTED to upstream.
var ErrQueueFull = errors.New("batcher: queue is full, backpressure applied")

// ErrMissingResponse is returned when the worker's batch response contains
// fewer items than expected.
var ErrMissingResponse = errors.New("batcher: worker returned fewer responses than requests")
