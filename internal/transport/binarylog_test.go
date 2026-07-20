package transport

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/binarylog"
	binlogpb "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// captureSink records every binary-log entry gRPC emits so the probe can scan
// the client-side ClientHeader metadata for the token.
type captureSink struct {
	mu      sync.Mutex
	entries []*binlogpb.GrpcLogEntry
}

func (s *captureSink) Write(e *binlogpb.GrpcLogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
	return nil
}

func (s *captureSink) Close() error { return nil }

// clientHeaderCount returns how many CLIENT-side ClientHeader entries were
// captured. Zero means binary logging was not actually active, which would make
// a "no leak" result meaningless — the probe asserts this is non-zero.
func (s *captureSink) clientHeaderCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.entries {
		if e.GetLogger() == binlogpb.GrpcLogEntry_LOGGER_CLIENT && e.GetClientHeader() != nil {
			n++
		}
	}
	return n
}

// clientHeaderLeaks reports whether the CLIENT-side binary log recorded the
// authorization header (by key or by the token value). Server-side entries are
// ignored: the server necessarily receives the token to authenticate the call;
// the leak this guards against is the token landing in the *client's* log.
func (s *captureSink) clientHeaderLeaks(value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.GetLogger() != binlogpb.GrpcLogEntry_LOGGER_CLIENT {
			continue
		}
		ch := e.GetClientHeader()
		if ch == nil {
			continue
		}
		for _, entry := range ch.GetMetadata().GetEntry() {
			if entry.GetKey() == "authorization" || strings.Contains(string(entry.GetValue()), value) {
				return true
			}
		}
	}
	return false
}

const binlogProbeSentinel = "sentinel-token-do-not-log-9f3a1c"

// TestAuthTokenNotInBinaryLog proves the Bearer token never lands in gRPC's
// client-side binary log, on both a unary and a streaming call. Because gRPC
// reads the binary-log filter from the GRPC_BINARY_LOG_FILTER environment
// variable at package-init time, the probe re-execs itself as a child with that
// variable set, installs a capturing binlog sink, runs the calls with a sentinel
// token, and asserts (a) client ClientHeaders were actually captured — so a
// "no leak" result is meaningful — and (b) the sentinel and the authorization
// key never appear in any client ClientHeader. It fails if the token is carried
// as ordinary outgoing metadata and passes once it rides as per-RPC credentials.
func TestAuthTokenNotInBinaryLog(t *testing.T) {
	if os.Getenv("BINLOG_PROBE_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run", "^TestAuthTokenNotInBinaryLog$", "-test.v")
		cmd.Env = append(os.Environ(), "BINLOG_PROBE_CHILD=1", "GRPC_BINARY_LOG_FILTER=*")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("child probe failed: %v\n%s", err, out)
		}
		if !bytes.Contains(out, []byte("BINLOG_PROBE_RAN")) {
			t.Fatalf("child probe did not run its assertion:\n%s", out)
		}
		return
	}

	sink := &captureSink{}
	binarylog.SetSink(sink)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(bufServerOpt)
	usersFake := &fakeUsers{}
	streamFake := &fakeMarketStream{metadata: make(chan metadata.MD, 1)}
	investapi.RegisterUsersServiceServer(srv, usersFake)
	investapi.RegisterMarketDataStreamServiceServer(srv, streamFake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := Dial(context.Background(), Config{Endpoint: "passthrough:///bufnet", Token: binlogProbeSentinel, CAFile: bufCAFile},
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Unary call: the server must still receive the token.
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	usersFake.mu.Lock()
	gotAuth := usersFake.gotMD.Get("authorization")
	usersFake.mu.Unlock()
	if len(gotAuth) != 1 || gotAuth[0] != "Bearer "+binlogProbeSentinel {
		t.Fatalf("unary server authorization = %v, want the token delivered", gotAuth)
	}

	// Streaming call: the token must also reach the stream server.
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream, err := investapi.NewMarketDataStreamServiceClient(conn).MarketDataStream(streamCtx)
	if err != nil {
		t.Fatalf("MarketDataStream: %v", err)
	}
	if err := stream.Send(&investapi.MarketDataRequest{}); err != nil {
		t.Fatalf("stream Send: %v", err)
	}
	select {
	case md := <-streamFake.metadata:
		if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+binlogProbeSentinel {
			t.Fatalf("stream server authorization = %v, want the token delivered", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream handler did not receive metadata")
	}

	// Binary logging must have actually captured client headers...
	if n := sink.clientHeaderCount(); n < 2 {
		t.Fatalf("captured %d client ClientHeaders, want >= 2 (unary + stream); binary logging may be inactive", n)
	}
	// ...and none may carry the token.
	if sink.clientHeaderLeaks(binlogProbeSentinel) {
		t.Fatalf("token leaked into the client-side binary log ClientHeader")
	}
	t.Log("BINLOG_PROBE_RAN")
}
