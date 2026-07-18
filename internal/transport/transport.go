// Package transport owns the gRPC connection and interceptors: Bearer auth,
// per-call deadlines, call-phase tracking, and x-tracking-id capture. A retry
// interceptor (ported from the official Go SDK, see docs/sdk-spike.md) will be
// chained in via Config.Retry once M1 lands.
package transport

import (
	"context"
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
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
	// Retry is the seam for the idempotency-aware retry interceptor planned in
	// M1 (docs/sdk-spike.md). When set it is chained after auth/app-name, so
	// every attempt carries full metadata and phase tracking still observes
	// the wire.
	Retry grpc.UnaryClientInterceptor
	// Credentials overrides transport security; nil means TLS with system
	// roots. Tests inject insecure credentials here.
	Credentials credentials.TransportCredentials
}

// Dial returns a client connection with the full interceptor chain installed.
// The connection is lazy: no network traffic happens until the first call, so
// ctx is accepted for signature stability but not consumed today.
func Dial(_ context.Context, cfg Config, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	creds := cfg.Credentials
	if creds == nil {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
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
	if cfg.Retry != nil {
		chain = append(chain, cfg.Retry)
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(chain...),
		grpc.WithStatsHandler(phaseStats{}),
	}
	opts = append(opts, extra...)
	return grpc.NewClient(cfg.Endpoint, opts...)
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
