// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.
//
// Package retry is an idempotency-aware gRPC unary client retry interceptor.
//
// The retry loop is adapted from
// github.com/russianinvestments/invest-api-go-sdk (tag v1.40.1, commit
// 9c992f69a3a6ba3ed308e04e3b685eb6c550a109), package retry/retry.go, which is
// itself a vendored copy of go-grpc-middleware/v2's gRPC retry interceptor
// (see NOTICE and THIRD_PARTY_LICENSES.md at the repository root for full
// provenance). This file merges upstream's UnaryClientInterceptor and
// UnaryClientInterceptorRE into a single loop driven by one RetryPolicy and a
// shared attempt budget, adds jitter to the RESOURCE_EXHAUSTED backoff
// (missing upstream — docs/sdk-spike.md §1), and gates retries on
// idempotency (idempotent.go, methods.go): a call is retried only if it is a
// recognized read RPC or the caller opted in via Idempotent. Upstream has no
// idempotency concept and retries indiscriminately by gRPC code. This
// package has no dependency on go-grpc-middleware, the upstream SDK's
// config/logger types, or the generated proto package — only
// google.golang.org/grpc, codes, and status, matching upstream's own
// proto-agnostic design (docs/sdk-spike.md §1).
package retry

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Backoff tuning for DefaultRetryPolicy: base 100ms, capped at 5s, doubling
// per attempt, +/-20% jitter (docs/plan-tinvest-cli.md §9: "reads →
// automatic retries with exponential backoff + jitter").
const (
	defaultBackoffBase    = 100 * time.Millisecond
	defaultBackoffCap     = 5 * time.Second
	defaultJitterFraction = 0.20
)

// RetryPolicy configures the retry interceptor: how many attempts are made,
// which gRPC codes are retried on top of the idempotency gate, and whether
// RESOURCE_EXHAUSTED responses honor the broker's rate-limit trailer.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first.
	// Zero disables retries entirely (the interceptor becomes a pass-through).
	MaxAttempts uint
	// PerCallCodes lists gRPC codes eligible for retry in addition to
	// RESOURCE_EXHAUSTED, which is governed separately by RateLimitRetry.
	PerCallCodes []codes.Code
	// RateLimitRetry, when true, retries RESOURCE_EXHAUSTED responses,
	// honoring the broker's x-ratelimit-reset trailer as the wait before
	// the next attempt (jittered). When the trailer is absent or
	// unparseable, RESOURCE_EXHAUSTED falls back to being retried only if
	// it also appears in PerCallCodes.
	RateLimitRetry bool
}

// DefaultRetryPolicy is the CLI's default retry policy
// (docs/plan-tinvest-cli.md §9): 3 attempts, UNAVAILABLE plus rate-limit
// retry, jittered exponential backoff (base 100ms, cap 5s).
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		PerCallCodes:   []codes.Code{codes.Unavailable},
		RateLimitRetry: true,
	}
}

// rateLimitResetTrailer is the metadata key the broker sends alongside a
// RESOURCE_EXHAUSTED status, carrying the number of seconds until the
// per-token rate limit resets.
const rateLimitResetTrailer = "x-ratelimit-reset"

// NewUnaryClientInterceptor builds a gRPC unary client interceptor that
// retries per policy. It is meant to be chained after auth/app-name
// metadata interceptors (transport.Config.Retry's contract) so every retried
// attempt still carries full outgoing metadata, and before/alongside the
// transport's phase-tracking stats.Handler, which observes every attempt on
// the wire independently (see internal/transport/phase.go: CallInfo is keyed off
// the call's context, shared by every attempt). Its confirmed flag is reset at
// each attempt's stats.Begin so the classification reflects the FINAL attempt's
// outcome, while sent stays a high-water mark, so an earlier attempt that
// confirmed cannot mask a final attempt that ended sent_unconfirmed (F1).
func NewUnaryClientInterceptor(policy RetryPolicy) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if policy.MaxAttempts == 0 || !retryEligible(ctx, method) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		backoff := exponentialJitterBackoff(defaultBackoffBase, defaultBackoffCap, defaultJitterFraction)

		var lastErr error
		var fixedWait time.Duration
		useFixedWait := false
		for attempt := uint(0); attempt < policy.MaxAttempts; attempt++ {
			if attempt > 0 {
				wait := backoff(attempt)
				if useFixedWait {
					wait = jitterUp(fixedWait, defaultJitterFraction)
					useFixedWait = false
				}
				if err := sleep(ctx, wait); err != nil {
					return err
				}
			}

			var trailer metadata.MD
			attemptOpts := make([]grpc.CallOption, 0, len(opts)+1)
			attemptOpts = append(attemptOpts, opts...)
			attemptOpts = append(attemptOpts, grpc.Trailer(&trailer))

			lastErr = invoker(ctx, method, req, reply, cc, attemptOpts...)
			if lastErr == nil {
				return nil
			}
			if local, ok := lastErr.(interface{ NoRetry() bool }); ok && local.NoRetry() {
				return lastErr
			}

			code := status.Code(lastErr)
			if code == codes.ResourceExhausted && policy.RateLimitRetry {
				if d, ok := rateLimitReset(trailer); ok {
					fixedWait = d
					useFixedWait = true
					continue
				}
			}
			if !retryableCode(code, policy.PerCallCodes) {
				return lastErr
			}
		}
		return lastErr
	}
}

func retryableCode(code codes.Code, retryable []codes.Code) bool {
	for _, c := range retryable {
		if c == code {
			return true
		}
	}
	return false
}

// rateLimitReset parses the x-ratelimit-reset trailer the broker sends with
// RESOURCE_EXHAUSTED responses (whole seconds until the limit resets).
func rateLimitReset(md metadata.MD) (time.Duration, bool) {
	vals := md.Get(rateLimitResetTrailer)
	if len(vals) == 0 {
		return 0, false
	}
	secs, err := strconv.Atoi(vals[0])
	if err != nil || secs < 0 {
		return 0, false
	}
	return time.Duration(secs) * time.Second, true
}

// sleep waits for d or returns ctx's error if ctx is done first.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
