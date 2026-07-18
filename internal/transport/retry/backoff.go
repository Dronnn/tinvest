// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.
//
// Adapted from github.com/russianinvestments/invest-api-go-sdk (tag v1.40.1,
// commit 9c992f69a3a6ba3ed308e04e3b685eb6c550a109, package retry/backoff.go),
// itself a vendored copy of go-grpc-middleware/v2's gRPC retry interceptor.
// See NOTICE and THIRD_PARTY_LICENSES.md at the repository root for full
// provenance. Changes from upstream: upstream's BackoffExponential has no
// jitter and no cap (docs/sdk-spike.md §1 flags this as a thundering-herd
// risk for the RESOURCE_EXHAUSTED path in particular); this file adds both.

package retry

import (
	"math/rand"
	"time"
)

// jitterUp returns duration adjusted by a random +/-jitterFraction: for a 10s
// duration and jitterFraction 0.2, a value in [8s, 12s].
func jitterUp(duration time.Duration, jitterFraction float64) time.Duration {
	multiplier := jitterFraction * (rand.Float64()*2 - 1)
	return time.Duration(float64(duration) * (1 + multiplier))
}

// exponentBase2 computes 2^(attempt-1) for attempt >= 1, and 0 for attempt == 0.
func exponentBase2(attempt uint) uint {
	return (1 << attempt) >> 1
}

// backoffFunc returns the wait before the given (1-based) retry attempt.
type backoffFunc func(attempt uint) time.Duration

// exponentialJitterBackoff grows the wait exponentially from base, capped at
// capAt, with +/-jitterFraction random jitter applied after capping so the
// cap itself can't produce a thundering herd. Neither the cap nor the jitter
// exist in upstream's BackoffExponential.
func exponentialJitterBackoff(base, capAt time.Duration, jitterFraction float64) backoffFunc {
	return func(attempt uint) time.Duration {
		d := base * time.Duration(exponentBase2(attempt))
		if d <= 0 || d > capAt {
			d = capAt
		}
		return jitterUp(d, jitterFraction)
	}
}
