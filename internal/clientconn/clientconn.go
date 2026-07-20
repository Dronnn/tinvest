// Package clientconn assembles the shared gRPC connection used by both the
// CLI and the public library facade. It centralizes the interceptor stack the
// broker layer depends on — Bearer auth, per-call deadlines, call-phase and
// x-tracking-id capture, the idempotency-aware retry policy, and client-side
// rate limiting — so the two front ends dial the broker identically.
package clientconn

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/Dronnn/tinvest/internal/ratelimit"
	"github.com/Dronnn/tinvest/internal/transport"
	"github.com/Dronnn/tinvest/internal/transport/retry"
)

// Canonical API hosts. These mirror config.ProdEndpoint and
// config.SandboxEndpoint (an equality test pins them together) and live here so
// the library facade can name an endpoint through the connection layer without
// importing internal/config — which would drag the TOML config parser into
// library consumers' dependency graphs.
const (
	// ProdEndpoint is the production host:port.
	ProdEndpoint = "invest-public-api.tbank.ru:443"
	// SandboxEndpoint is the sandbox host:port.
	SandboxEndpoint = "sandbox-invest-public-api.tbank.ru:443"
)

// Config describes a broker connection at the level both front ends share.
type Config struct {
	// Endpoint is the host:port to dial (see config.ProdEndpoint and
	// config.SandboxEndpoint for the canonical hosts).
	Endpoint string
	// Token is the Bearer token. It is only ever placed in the authorization
	// metadata of outgoing calls — never logged or echoed.
	Token string
	// Timeout is the per-call deadline for calls without one; zero means
	// transport.DefaultTimeout.
	Timeout time.Duration
	// CAFile, when non-empty, points to a PEM bundle used INSTEAD of the
	// system trust store to verify the server certificate. Hostname
	// verification is unaffected — only the root pool changes.
	CAFile string
	// DisableRateLimit turns off the process-local unary token buckets (the
	// CLI's --no-rate-limit path). When false, the default limiter is
	// installed and returned so the caller can prime it.
	DisableRateLimit bool
}

// Dial builds the connection with the default retry policy always enabled
// (eligibility is decided per call, so reads retry automatically and
// mutations only when the call site opts in) and, unless disabled, the
// default client-side rate limiter. The returned limiter is non-nil exactly
// when rate limiting is enabled, so callers that warm it up (e.g. the CLI's
// tariff refresh) have the handle; it is nil otherwise.
func Dial(ctx context.Context, cfg Config) (*grpc.ClientConn, *ratelimit.Limiter, error) {
	policy := retry.DefaultRetryPolicy()
	var limiter *ratelimit.Limiter
	if !cfg.DisableRateLimit {
		limiter = ratelimit.New(ratelimit.DefaultLimits(), ratelimit.DefaultMaxWait)
	}
	conn, err := transport.Dial(ctx, transport.Config{
		Endpoint:    cfg.Endpoint,
		Token:       cfg.Token,
		Timeout:     cfg.Timeout,
		CAFile:      cfg.CAFile,
		RetryPolicy: &policy,
		RateLimiter: limiter,
	})
	if err != nil {
		return nil, nil, err
	}
	return conn, limiter, nil
}
