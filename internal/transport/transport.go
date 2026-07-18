// Package transport owns the gRPC connection and interceptors: Bearer auth,
// per-call deadlines, call-phase tracking, and x-tracking-id capture. The
// idempotency-aware retry interceptor (ported from the official Go SDK, see
// docs/sdk-spike.md) lives in internal/transport/retry and is chained in via
// Config.Retry — set explicitly, or built automatically from Config.RetryPolicy.
package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"tinvest/internal/ratelimit"
	"tinvest/internal/transport/retry"
)

// DefaultTimeout is the per-call deadline applied when the caller's context
// carries none.
const DefaultTimeout = 10 * time.Second

// appName identifies this client to the broker, as recommended by the API docs.
const appName = "Dronnn.tinvest"

// Config describes a broker connection.
type Config struct {
	// Endpoint is the host:port to dial (see config.ProdEndpoint and
	// config.SandboxEndpoint for the canonical hosts).
	Endpoint string
	// Token is the Bearer token. It is only ever placed in the authorization
	// metadata of outgoing calls — never logged or echoed.
	Token string
	// Timeout is the per-call deadline for calls without one; zero means
	// DefaultTimeout.
	Timeout time.Duration
	// Retry is the seam for the idempotency-aware retry interceptor
	// (internal/transport/retry, docs/sdk-spike.md). When set it is chained
	// after auth/app-name, so every attempt carries full metadata and phase
	// tracking still observes the wire. Takes precedence over RetryPolicy —
	// set this directly for full control (e.g. tests injecting a stub).
	Retry grpc.UnaryClientInterceptor
	// RetryPolicy, when Retry is nil, builds the default retry interceptor
	// via retry.NewUnaryClientInterceptor. Leave both nil to disable retries.
	RetryPolicy *retry.RetryPolicy
	// RateLimiter, when non-nil, throttles every unary wire attempt inside the
	// retry loop. Nil disables client-side limiting (the --no-rate-limit path).
	RateLimiter *ratelimit.Limiter
	// Credentials overrides transport security; nil means TLS with system
	// roots. Tests inject insecure credentials here. When set, CAFile is
	// ignored — the caller has already decided how trust is established.
	Credentials credentials.TransportCredentials
	// CAFile, when non-empty, points to a PEM bundle (one or more
	// certificates, e.g. the Russian Trusted Root CA + Sub CA — see the
	// README's "Russian CA certificates" section) used INSTEAD of the system
	// trust store to verify the server certificate. Hostname verification is
	// unaffected: this only swaps the root pool, it never disables
	// verification. Ignored when Credentials is set.
	CAFile string
}

// Dial returns a client connection with the full interceptor chain installed.
// The connection is lazy: no network traffic happens until the first call, so
// ctx is accepted for signature stability but not consumed today.
func Dial(_ context.Context, cfg Config, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	creds := cfg.Credentials
	if creds == nil {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.CAFile != "" {
			pool, err := loadCAPool(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			// ServerName is left untouched: only the root pool changes, so
			// hostname verification stays normal (plan §14 forbids an
			// insecure-skip-verify option).
			tlsConfig.RootCAs = pool
		}
		creds = credentials.NewTLS(tlsConfig)
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	chain := []grpc.UnaryClientInterceptor{
		deadlineInterceptor(timeout),
		metadataInterceptor("authorization", "Bearer "+cfg.Token),
		metadataInterceptor("x-app-name", appName),
	}
	retryInterceptor := cfg.Retry
	if retryInterceptor == nil && cfg.RetryPolicy != nil {
		retryInterceptor = retry.NewUnaryClientInterceptor(*cfg.RetryPolicy)
	}
	if retryInterceptor != nil {
		chain = append(chain, retryInterceptor)
	}
	if cfg.RateLimiter != nil {
		chain = append(chain, cfg.RateLimiter.UnaryClientInterceptor())
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(chain...),
		grpc.WithChainStreamInterceptor(
			streamMetadataInterceptor("authorization", "Bearer "+cfg.Token),
			streamMetadataInterceptor("x-app-name", appName),
		),
		grpc.WithStatsHandler(phaseStats{}),
	}
	opts = append(opts, extra...)
	return grpc.NewClient(cfg.Endpoint, opts...)
}

// loadCAPool reads a PEM bundle (root + intermediate/sub CA certs, e.g. the
// Russian Trusted Root CA and Sub CA required to reach T-Bank's endpoints —
// see the README's "Russian CA certificates" section) and returns a cert
// pool built from it. Errors here are plain config-shaped errors (empty
// file, unreadable path, no valid PEM certificates), the same class as the
// other validation failures in internal/config: they map to the usage exit
// code, never to a network/auth one.
func loadCAPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA file %s: %w", path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("CA file %s is empty", path)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("CA file %s contains no valid PEM certificates", path)
	}
	return pool, nil
}

// deadlineInterceptor applies the default per-call deadline when the caller
// has not set one.
func deadlineInterceptor(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// metadataInterceptor appends a fixed key/value pair to outgoing metadata.
func metadataInterceptor(key, value string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, key, value)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamMetadataInterceptor adds authentication/app identity without a
// default deadline: stream lifetime is governed by cancellation and the ping
// watchdog, not the 10-second unary call timeout.
func streamMetadataInterceptor(key, value string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, key, value)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
