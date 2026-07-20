//go:build e2elive

package e2elive

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Dronnn/tinvest/internal/config"
)

// relay is a transparent, TLS-terminating gRPC forwarding proxy. It serves the
// CLI over TLS (a runtime-generated leaf the CLI trusts via TINVEST_CA_FILE) and
// forwards every method to the REAL sandbox over real TLS, copying the incoming
// metadata upstream. The one exception is the Bearer token: it is stripped from
// the forwarded metadata and re-attached as per-RPC credentials on the upstream
// call (see director), so — like the production transport — the token never
// rides in ordinary outgoing metadata. The relay never decodes protobuf: a raw
// passthrough codec moves already-encoded frames both ways, so a single generic
// handler forwards instrument resolution, tariff refresh, order placement, and
// reconciliation reads alike.
//
// The one non-transparent behavior is an optional hook on PostOrder: the hook
// fires the instant the broker's response reaches the relay (the order is by
// then created or rejected on the sandbox) and blocks before that response is
// relayed back to the CLI. That lets a test SIGKILL the CLI strictly after
// send-started is journaled and the request is on the wire, but strictly before
// any confirmation could be recorded — deterministically, with no sleep.
type relay struct {
	upstream *grpc.ClientConn
	server   *grpc.Server
	addr     string

	mu          sync.Mutex
	onPostOrder func() // fired once per PostOrder passthrough when set
}

// bearerCreds re-attaches the CLI's Bearer token to the upstream call as
// per-RPC credentials rather than ordinary metadata. RequireTransportSecurity
// reports true — the token is a secret and the upstream leg is real TLS.
type bearerCreds struct{ value string }

func (c bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": c.value}, nil
}

func (bearerCreds) RequireTransportSecurity() bool { return true }

// rawFrame is an opaque gRPC message: the already-encoded protobuf bytes.
type rawFrame struct{ payload []byte }

// rawCodec is a passthrough codec: it neither marshals nor unmarshals, it moves
// raw bytes. Installed on both legs so the relay forwards frames untouched.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*rawFrame)
	if !ok {
		return nil, status.Errorf(codes.Internal, "rawCodec: cannot marshal %T", v)
	}
	return f.payload, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*rawFrame)
	if !ok {
		return status.Errorf(codes.Internal, "rawCodec: cannot unmarshal into %T", v)
	}
	f.payload = data
	return nil
}

func (rawCodec) Name() string { return "tinvest-e2e-live-passthrough" }

// newRelay starts a relay on a fresh loopback port. Its upstream is hardwired to
// config.SandboxEndpoint (verified here as a sandbox host, never prod), verified
// with the operator's Russian CA bundle when provided. It is stopped at test end.
func newRelay(t *testing.T) *relay {
	t.Helper()
	if config.SandboxEndpoint == config.ProdEndpoint || !strings.Contains(config.SandboxEndpoint, "sandbox") {
		t.Fatalf("relay upstream %q is not a sandbox host; refusing", config.SandboxEndpoint)
	}

	upstream, err := grpc.NewClient(
		config.SandboxEndpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    sandboxRootPool, // nil => system roots
		})),
	)
	if err != nil {
		t.Fatalf("dial sandbox upstream: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = upstream.Close()
		t.Fatalf("listen: %v", err)
	}
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		_ = upstream.Close()
		t.Fatalf("split host/port: %v", err)
	}

	r := &relay{upstream: upstream, addr: "127.0.0.1:" + port}
	creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{relayServerCert}})
	r.server = grpc.NewServer(
		grpc.Creds(creds),
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(r.director),
	)

	go func() { _ = r.server.Serve(lis) }()
	t.Cleanup(func() {
		r.server.Stop()
		_ = upstream.Close()
	})
	return r
}

// setOnPostOrder installs the PostOrder passthrough hook under the mutex.
func (r *relay) setOnPostOrder(fn func()) {
	r.mu.Lock()
	r.onPostOrder = fn
	r.mu.Unlock()
}

func (r *relay) postOrderHook() func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.onPostOrder
}

// director forwards one stream to the sandbox and back. Unary calls are the
// degenerate one-frame-each-way case of the same pump.
func (r *relay) director(_ any, serverStream grpc.ServerStream) error {
	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Error(codes.Internal, "no method in server stream")
	}

	md, _ := metadata.FromIncomingContext(serverStream.Context())
	// Strip the bearer token from the forwarded metadata and re-attach it as
	// per-RPC credentials on the upstream call, mirroring the production
	// transport: the token is applied at the transport layer, below the point
	// where a gRPC binary logger records the outgoing ClientHeader, so it never
	// rides in ordinary outgoing metadata.
	forwarded := md.Copy()
	callOpts := []grpc.CallOption{grpc.ForceCodec(rawCodec{})}
	if auth := forwarded.Get("authorization"); len(auth) > 0 {
		forwarded.Delete("authorization")
		callOpts = append(callOpts, grpc.PerRPCCredentials(bearerCreds{value: auth[0]}))
	}
	outCtx, cancel := context.WithCancel(metadata.NewOutgoingContext(serverStream.Context(), forwarded))
	defer cancel()

	clientStream, err := r.upstream.NewStream(
		outCtx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		fullMethod,
		callOpts...,
	)
	if err != nil {
		return err
	}

	hook := strings.HasSuffix(fullMethod, "/PostOrder")
	s2c := r.forwardServerToClient(serverStream, clientStream)
	c2s := r.forwardClientToServer(clientStream, serverStream, hook)

	for i := 0; i < 2; i++ {
		select {
		case err := <-s2c:
			if err == io.EOF {
				// The CLI finished sending; half-close the upstream so the sandbox
				// processes the request and produces its response.
				_ = clientStream.CloseSend()
				continue
			}
			cancel()
			return status.Errorf(codes.Internal, "relay s2c: %v", err)
		case err := <-c2s:
			// Upstream is done: propagate its trailer and terminal status.
			serverStream.SetTrailer(clientStream.Trailer())
			if err != io.EOF {
				return err
			}
			return nil
		}
	}
	return status.Error(codes.Internal, "relay: unreachable")
}

// forwardServerToClient pumps request frames from the CLI to the sandbox.
func (r *relay) forwardServerToClient(src grpc.ServerStream, dst grpc.ClientStream) chan error {
	ret := make(chan error, 1)
	go func() {
		f := &rawFrame{}
		for {
			if err := src.RecvMsg(f); err != nil { // io.EOF when the CLI half-closes
				ret <- err
				return
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				return
			}
		}
	}()
	return ret
}

// forwardClientToServer pumps response frames from the sandbox back to the CLI.
// On the first frame it also forwards headers, and — for PostOrder — invokes the
// hook after the response is in hand but before it is relayed, so a test can kill
// the CLI with the broker's outcome already durable but unseen by the CLI.
func (r *relay) forwardClientToServer(src grpc.ClientStream, dst grpc.ServerStream, hook bool) chan error {
	ret := make(chan error, 1)
	go func() {
		f := &rawFrame{}
		for i := 0; ; i++ {
			if err := src.RecvMsg(f); err != nil { // io.EOF on a clean upstream end
				ret <- err
				return
			}
			if i == 0 {
				h, err := src.Header()
				if err != nil {
					ret <- err
					return
				}
				if err := dst.SendHeader(h); err != nil {
					ret <- err
					return
				}
				if hook {
					if fn := r.postOrderHook(); fn != nil {
						fn()
					}
				}
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				return
			}
		}
	}()
	return ret
}
