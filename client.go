package tinvest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"

	brokerinstruments "github.com/Dronnn/tinvest/internal/broker/instruments"
	brokermarketdata "github.com/Dronnn/tinvest/internal/broker/marketdata"
	brokerresearch "github.com/Dronnn/tinvest/internal/broker/research"
	"github.com/Dronnn/tinvest/internal/clientconn"
	"github.com/Dronnn/tinvest/internal/transport"
)

// Canonical API endpoints, re-exported so callers can pin an endpoint without
// depending on an internal package.
const (
	// ProdEndpoint is the production host:port.
	ProdEndpoint = clientconn.ProdEndpoint
	// SandboxEndpoint is the sandbox host:port.
	SandboxEndpoint = clientconn.SandboxEndpoint
)

// Config configures a Client. Token is required; every other field has a safe
// default (production endpoint, 10s per-call timeout, rate limiter and
// instrument cache both on).
type Config struct {
	// Token is the T-Invest API token. It is required and is only ever placed
	// in the authorization metadata of outgoing calls — never logged.
	Token string
	// Sandbox selects the sandbox endpoint. Ignored when Endpoint is set.
	Sandbox bool
	// Endpoint overrides the host:port to dial. Empty means the production or
	// sandbox endpoint per Sandbox.
	Endpoint string
	// CAFile, when non-empty, is a path to a PEM bundle used in place of the
	// system trust store to verify the server certificate — the same trust
	// behavior as the CLI's TINVEST_CA_FILE, including a leading "~/" that
	// expands to the user's home directory. Hostname verification is
	// unaffected; only the root pool changes.
	CAFile string
	// Timeout is the per-call deadline applied to calls whose context carries
	// none. Zero means the default (10s).
	Timeout time.Duration
	// DisableRateLimit turns off the process-local client-side rate limiter.
	// The limiter is on by default.
	DisableRateLimit bool
	// DisableCache turns off the local instrument-resolution cache (a JSON file
	// under the user cache directory; see instruments.DefaultCachePath for the
	// exact location and why platform-native dirs are not used). The cache is
	// on by default. Known limitation: concurrent processes sharing the cache
	// file can race on write (last writer wins) through the shared .tmp path,
	// which can also cause a rename failure or transient malformed cache
	// content. The cache is best-effort: any such failure degrades to a cache
	// miss (a re-resolution), and results stay correct.
	DisableCache bool
}

// Client is a read-only T-Invest API client over one shared gRPC connection.
// It is safe for concurrent use. Call Close to release the connection.
type Client struct {
	conn        *grpc.ClientConn
	instruments brokerinstruments.Client
	marketdata  brokermarketdata.Client
	research    brokerresearch.Client
}

// New dials the broker and returns a ready Client. The connection is lazy: no
// network traffic happens until the first method call, so New only fails for a
// missing token, an unreadable CA bundle, or a malformed endpoint. Call Close
// when done.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("tinvest: token is required")
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = ProdEndpoint
		if cfg.Sandbox {
			endpoint = SandboxEndpoint
		}
	}
	caFile, err := expandHome(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	conn, _, err := clientconn.Dial(ctx, clientconn.Config{
		Endpoint:         endpoint,
		Token:            cfg.Token,
		Timeout:          cfg.Timeout,
		CAFile:           caFile,
		DisableRateLimit: cfg.DisableRateLimit,
	})
	if err != nil {
		return nil, err
	}
	var cache *brokerinstruments.Cache
	if !cfg.DisableCache {
		cache = brokerinstruments.NewCache(brokerinstruments.DefaultCachePath(), brokerinstruments.DefaultTTL, nil)
	}
	return newClient(conn, cache), nil
}

// newClient wires the broker sub-clients over an established connection. It is
// the seam bufconn tests use to inject an in-process connection.
func newClient(conn *grpc.ClientConn, cache *brokerinstruments.Cache) *Client {
	return &Client{
		conn:        conn,
		instruments: brokerinstruments.New(conn, cache),
		marketdata:  brokermarketdata.New(conn),
		research:    brokerresearch.New(conn),
	}
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// resolveUID resolves one identifier (uid, FIGI, or TICKER@CLASSCODE) to its
// instrument_uid, mirroring how every CLI read command normalizes its
// arguments before a data call. A malformed identifier is caught locally
// (GRPCCode Unknown); a broker failure carries the tracking id — both as an
// *APIError.
func (c *Client) resolveUID(ctx context.Context, id string) (string, error) {
	ctx, info := transport.WithCallInfo(ctx)
	inst, err := c.instruments.Resolve(ctx, id, false)
	if err != nil {
		return "", apiErr(err, info)
	}
	return inst.GetUid(), nil
}

// resolveUIDs resolves every identifier in order, stopping at the first
// failure.
func (c *Client) resolveUIDs(ctx context.Context, ids []string) ([]string, error) {
	uids := make([]string, 0, len(ids))
	for _, id := range ids {
		uid, err := c.resolveUID(ctx, id)
		if err != nil {
			return nil, err
		}
		uids = append(uids, uid)
	}
	return uids, nil
}

// expandHome resolves a leading "~/" to the user's home directory, matching
// the CLI's TINVEST_CA_FILE handling. Other paths (and the empty path) are
// returned unchanged.
func expandHome(path string) (string, error) {
	if !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, path[2:]), nil
}
