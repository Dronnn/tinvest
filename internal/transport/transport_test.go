package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/ratelimit"
	"tinvest/internal/transport/retry"
)

// fakeUsers is an in-process UsersService capturing what the client sent.
type fakeUsers struct {
	investapi.UnimplementedUsersServiceServer

	mu          sync.Mutex
	gotMD       metadata.MD
	hadDeadline bool
	deadlineIn  time.Duration

	handler func(ctx context.Context) (*investapi.GetAccountsResponse, error)
}

type fakeMarketStream struct {
	investapi.UnimplementedMarketDataStreamServiceServer
	metadata chan metadata.MD
}

func (f *fakeMarketStream) MarketDataStream(stream grpc.BidiStreamingServer[investapi.MarketDataRequest, investapi.MarketDataResponse]) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	f.metadata <- md
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (f *fakeUsers) GetAccounts(ctx context.Context, _ *investapi.GetAccountsRequest) (*investapi.GetAccountsResponse, error) {
	f.mu.Lock()
	f.gotMD, _ = metadata.FromIncomingContext(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		f.hadDeadline = true
		f.deadlineIn = time.Until(deadline)
	}
	handler := f.handler
	f.mu.Unlock()
	if handler != nil {
		return handler(ctx)
	}
	return &investapi.GetAccountsResponse{}, nil
}

func startServer(t *testing.T, f *fakeUsers) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterUsersServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis
}

func dialBuf(t *testing.T, lis *bufconn.Listener, cfg Config) *grpc.ClientConn {
	t.Helper()
	cfg.Endpoint = "passthrough:///bufnet"
	cfg.Credentials = insecure.NewCredentials()
	conn, err := Dial(context.Background(), cfg,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestAuthAndAppNameMetadata(t *testing.T) {
	fake := &fakeUsers{}
	lis := startServer(t, fake)
	conn := dialBuf(t, lis, Config{Token: "test-token"})

	ctx, info := WithCallInfo(context.Background())
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.gotMD.Get("authorization"); len(got) != 1 || got[0] != "Bearer test-token" {
		t.Errorf("authorization metadata = %v, want Bearer test-token", got)
	}
	if got := fake.gotMD.Get("x-app-name"); len(got) != 1 || got[0] != "Dronnn.tinvest" {
		t.Errorf("x-app-name metadata = %v, want Dronnn.tinvest", got)
	}
	if !fake.hadDeadline {
		t.Error("call had no deadline; default must apply")
	}
	if fake.deadlineIn > DefaultTimeout {
		t.Errorf("deadline %v further out than the default %v", fake.deadlineIn, DefaultTimeout)
	}
	if info.Phase() != PhaseConfirmed {
		t.Errorf("phase = %s, want confirmed", info.Phase())
	}
}

func TestAuthAndAppNameMetadataOnStreams(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	fake := &fakeMarketStream{metadata: make(chan metadata.MD, 1)}
	server := grpc.NewServer()
	investapi.RegisterMarketDataStreamServiceServer(server, fake)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := Dial(context.Background(), Config{
		Endpoint: "passthrough:///bufnet", Token: "stream-token", Credentials: insecure.NewCredentials(),
	}, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	client, err := investapi.NewMarketDataStreamServiceClient(conn).MarketDataStream(ctx)
	if err != nil {
		t.Fatalf("MarketDataStream: %v", err)
	}
	defer cancel()
	if err := client.Send(&investapi.MarketDataRequest{}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case md := <-fake.metadata:
		if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer stream-token" {
			t.Errorf("authorization = %v", got)
		}
		if got := md.Get("x-app-name"); len(got) != 1 || got[0] != appName {
			t.Errorf("x-app-name = %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("stream handler did not receive metadata")
	}
}

func TestTrackingIDCaptureOnSuccess(t *testing.T) {
	fake := &fakeUsers{handler: func(ctx context.Context) (*investapi.GetAccountsResponse, error) {
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-tracking-id", "trk-header"))
		return &investapi.GetAccountsResponse{}, nil
	}}
	lis := startServer(t, fake)
	conn := dialBuf(t, lis, Config{Token: "t"})

	ctx, info := WithCallInfo(context.Background())
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	if info.TrackingID() != "trk-header" {
		t.Errorf("tracking id = %q, want trk-header", info.TrackingID())
	}
}

func TestServerErrorIsConfirmedWithTrailerCapture(t *testing.T) {
	fake := &fakeUsers{handler: func(ctx context.Context) (*investapi.GetAccountsResponse, error) {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(
			"x-tracking-id", "trk-err",
			"message", "Invalid parameter value",
			"x-ratelimit-reset", "7",
		))
		return nil, status.Error(codes.InvalidArgument, "30001")
	}}
	lis := startServer(t, fake)
	conn := dialBuf(t, lis, Config{Token: "t"})

	ctx, info := WithCallInfo(context.Background())
	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	if info.Phase() != PhaseConfirmed {
		t.Errorf("phase = %s, want confirmed (server delivered a definitive status)", info.Phase())
	}
	if info.TrackingID() != "trk-err" {
		t.Errorf("tracking id = %q, want trk-err", info.TrackingID())
	}
	if info.APIMessage() != "Invalid parameter value" {
		t.Errorf("api message = %q", info.APIMessage())
	}
	if info.RetryAfter() != 7*time.Second {
		t.Errorf("retry after = %v, want 7s", info.RetryAfter())
	}
}

func TestDeadlineAfterSendStaysSentUnconfirmed(t *testing.T) {
	received := make(chan struct{})
	fake := &fakeUsers{handler: func(ctx context.Context) (*investapi.GetAccountsResponse, error) {
		close(received)
		<-ctx.Done() // hold the call past the client deadline
		return nil, ctx.Err()
	}}
	lis := startServer(t, fake)
	conn := dialBuf(t, lis, Config{Token: "t", Timeout: 200 * time.Millisecond})

	ctx, info := WithCallInfo(context.Background())
	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("server never received the request")
	}
	if info.Phase() != PhaseSentUnconfirmed {
		t.Errorf("phase = %s, want sent_unconfirmed (deadline fired after send)", info.Phase())
	}
}

func TestDialFailureIsNotSent(t *testing.T) {
	cfg := Config{Endpoint: "passthrough:///unreachable", Token: "t"}
	cfg.Credentials = insecure.NewCredentials()
	conn, err := Dial(context.Background(), cfg,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("no route")
		}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx, info := WithCallInfo(callCtx)
	_, err = investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if err == nil {
		t.Fatal("want error for unreachable endpoint")
	}
	if info.Phase() != PhaseNotSent {
		t.Errorf("phase = %s, want not_sent", info.Phase())
	}
}

func TestCallerDeadlineIsKept(t *testing.T) {
	fake := &fakeUsers{}
	lis := startServer(t, fake)
	conn := dialBuf(t, lis, Config{Token: "t"})

	callCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx, _ := WithCallInfo(callCtx)
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.hadDeadline {
		t.Fatal("caller deadline lost")
	}
	if fake.deadlineIn <= DefaultTimeout {
		t.Errorf("deadline %v: the caller's 1m deadline was replaced by the default", fake.deadlineIn)
	}
}

func TestRetrySeamIsChained(t *testing.T) {
	fake := &fakeUsers{}
	lis := startServer(t, fake)

	var retryCalls int
	retry := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		retryCalls++
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	conn := dialBuf(t, lis, Config{Token: "t", Retry: retry})

	ctx, _ := WithCallInfo(context.Background())
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	if retryCalls != 1 {
		t.Errorf("retry interceptor ran %d times, want 1", retryCalls)
	}

	// The seam sits inside auth/app-name: attempts issued by a future retry
	// interceptor must still carry full metadata.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.gotMD.Get("authorization"); len(got) != 1 {
		t.Errorf("authorization metadata missing through the retry seam: %v", got)
	}
}

func TestRateLimiterIsChainedOutsideUnaryCall(t *testing.T) {
	fake := &fakeUsers{}
	lis := startServer(t, fake)
	limiter := ratelimit.New([]ratelimit.Limit{{
		Group: "users", Methods: []string{investapi.UsersService_GetAccounts_FullMethodName},
		PerSecond: 20, Burst: 1,
	}}, 200*time.Millisecond)
	conn := dialBuf(t, lis, Config{Token: "t", RateLimiter: limiter})
	client := investapi.NewUsersServiceClient(conn)

	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("first GetAccounts: %v", err)
	}
	start := time.Now()
	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("second GetAccounts: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("second call waited %v, want limiter delay", elapsed)
	}
}

func TestRateLimiterChargesEveryRetryAttempt(t *testing.T) {
	var calls int32
	fake := &fakeUsers{handler: func(context.Context) (*investapi.GetAccountsResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, status.Error(codes.Unavailable, "retry")
		}
		return &investapi.GetAccountsResponse{}, nil
	}}
	lis := startServer(t, fake)
	limiter := ratelimit.New([]ratelimit.Limit{{
		Group: "users", Methods: []string{investapi.UsersService_GetAccounts_FullMethodName},
		PerSecond: 10, Burst: 1,
	}}, 250*time.Millisecond)
	retryImmediately := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := invoker(ctx, method, req, reply, cc, opts...); err == nil {
			return nil
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	conn := dialBuf(t, lis, Config{Token: "t", RateLimiter: limiter, Retry: retryImmediately})

	started := time.Now()
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 85*time.Millisecond {
		t.Fatalf("retry completed in %v, want second token to delay the wire attempt", elapsed)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

// TestRetryPolicyBuildsInterceptorAndPreservesCallInfo exercises the full
// Config.RetryPolicy wiring (internal/transport/retry, chained via Dial) end
// to end: the first attempt fails UNAVAILABLE (a real trailer is delivered,
// since it comes from an actual server handler over the bufconn transport),
// the retry succeeds, and CallInfo must reflect the final attempt only —
// phase confirmed, tracking id and rate-limit reset from the successful
// attempt, not stale/duplicated data from the failed one.
func TestRetryPolicyBuildsInterceptorAndPreservesCallInfo(t *testing.T) {
	var calls int32
	fake := &fakeUsers{handler: func(ctx context.Context) (*investapi.GetAccountsResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			_ = grpc.SetTrailer(ctx, metadata.Pairs("x-tracking-id", "trk-failed-attempt"))
			return nil, status.Error(codes.Unavailable, "try again")
		}
		_ = grpc.SetTrailer(ctx, metadata.Pairs("x-tracking-id", "trk-ok"))
		return &investapi.GetAccountsResponse{}, nil
	}}
	lis := startServer(t, fake)
	policy := retry.DefaultRetryPolicy()
	conn := dialBuf(t, lis, Config{Token: "t", RetryPolicy: &policy})

	ctx, info := WithCallInfo(context.Background())
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2 (one retry after UNAVAILABLE)", got)
	}
	if info.Phase() != PhaseConfirmed {
		t.Errorf("phase = %s, want confirmed", info.Phase())
	}
	if info.TrackingID() != "trk-ok" {
		t.Errorf("tracking id = %q, want trk-ok (the successful attempt's, not the failed attempt's)", info.TrackingID())
	}
}

// TestRetryPolicyNilLeavesRetryDisabled documents that RetryPolicy is opt-in:
// Dial does not retry anything when both Retry and RetryPolicy are nil.
func TestRetryPolicyNilLeavesRetryDisabled(t *testing.T) {
	var calls int32
	fake := &fakeUsers{handler: func(context.Context) (*investapi.GetAccountsResponse, error) {
		atomic.AddInt32(&calls, 1)
		return nil, status.Error(codes.Unavailable, "try again")
	}}
	lis := startServer(t, fake)
	conn := dialBuf(t, lis, Config{Token: "t"})

	ctx, _ := WithCallInfo(context.Background())
	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (no RetryPolicy set)", got)
	}
}

// --- Custom CA bundle (plan §14): a self-signed test root CA + server cert,
// generated at runtime, standing in for the Russian Trusted Root/Sub CA. ---

// testCA holds a freshly minted self-signed root CA and a server certificate
// it issued, all in PEM form for use by the test TLS listener and by CAFile.
type testCA struct {
	rootPEM []byte
	certPEM []byte
	keyPEM  []byte
}

// generateTestCA builds a throwaway root CA and a server certificate signed
// by it, valid for 127.0.0.1. This stands in for a real trust chain (e.g.
// the Russian Trusted Root CA + Sub CA) without depending on any external
// certificate authority.
func generateTestCA(t *testing.T) testCA {
	t.Helper()

	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate root key: %v", err)
	}
	rootTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tinvest-test Root CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatalf("create root cert: %v", err)
	}
	rootCert, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatalf("parse root cert: %v", err)
	}

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, rootCert, &serverKey.PublicKey, rootKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}

	return testCA{
		rootPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER}),
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER}),
	}
}

// writeCAFile writes the root CA PEM to a temp file and returns its path.
func writeCAFile(t *testing.T, pemBytes []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return path
}

// startTLSServer starts a real TLS-terminated gRPC server on 127.0.0.1
// presenting the given server certificate, and returns its address.
func startTLSServer(t *testing.T, f *fakeUsers, ca testCA) string {
	t.Helper()
	cert, err := tls.X509KeyPair(ca.certPEM, ca.keyPEM)
	if err != nil {
		t.Fatalf("load server keypair: %v", err)
	}
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	tlsLis := tls.NewListener(rawLis, &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2"}, // ALPN: grpc-go 1.67+ enforces this (see grpc/grpc-go#434)
	})
	srv := grpc.NewServer()
	investapi.RegisterUsersServiceServer(srv, f)
	go func() { _ = srv.Serve(tlsLis) }()
	t.Cleanup(srv.Stop)
	return rawLis.Addr().String()
}

// TestCustomCAPoolAllowsConnection proves that a connection succeeds against
// a server presenting a certificate signed by an otherwise-untrusted root,
// once that root is supplied via Config.CAFile.
func TestCustomCAPoolAllowsConnection(t *testing.T) {
	ca := generateTestCA(t)
	fake := &fakeUsers{}
	addr := startTLSServer(t, fake, ca)
	caFile := writeCAFile(t, ca.rootPEM)

	conn, err := Dial(context.Background(), Config{Endpoint: addr, Token: "t", CAFile: caFile})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, info := WithCallInfo(context.Background())
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts with custom CA pool: %v", err)
	}
	if info.Phase() != PhaseConfirmed {
		t.Errorf("phase = %s, want confirmed", info.Phase())
	}
}

// TestWithoutCustomCAPoolFailsCleanly proves the system pool is still what's
// used when CAFile is unset — a self-signed test root must not be trusted.
func TestWithoutCustomCAPoolFailsCleanly(t *testing.T) {
	ca := generateTestCA(t)
	fake := &fakeUsers{}
	addr := startTLSServer(t, fake, ca)

	conn, err := Dial(context.Background(), Config{Endpoint: addr, Token: "t"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, _ := WithCallInfo(context.Background())
	_, err = investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if err == nil {
		t.Fatal("want error: self-signed server cert must not be trusted without CAFile")
	}
}

// TestGarbageCAFileFailsAtDial proves an unparseable CA bundle fails fast,
// synchronously in Dial, with a plain config-shaped error (never silently
// falling back to no verification).
func TestGarbageCAFileFailsAtDial(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "garbage.pem")
	if err := os.WriteFile(caFile, []byte("this is not a PEM certificate"), 0o600); err != nil {
		t.Fatalf("write garbage CA file: %v", err)
	}

	_, err := Dial(context.Background(), Config{Endpoint: "127.0.0.1:0", Token: "t", CAFile: caFile})
	if err == nil {
		t.Fatal("want error for unparseable CA file")
	}
}

// TestEmptyCAFileFailsAtDial proves an empty CA bundle is rejected rather
// than silently producing an empty (trust-nothing, or worse, ambiguous) pool.
func TestEmptyCAFileFailsAtDial(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(caFile, []byte(""), 0o600); err != nil {
		t.Fatalf("write empty CA file: %v", err)
	}

	_, err := Dial(context.Background(), Config{Endpoint: "127.0.0.1:0", Token: "t", CAFile: caFile})
	if err == nil {
		t.Fatal("want error for empty CA file")
	}
}

// TestMissingCAFileFailsAtDial proves a nonexistent path is a clean error,
// not a panic or a silent fallback to the system pool.
func TestMissingCAFileFailsAtDial(t *testing.T) {
	_, err := Dial(context.Background(), Config{Endpoint: "127.0.0.1:0", Token: "t", CAFile: "/nonexistent/ca.pem"})
	if err == nil {
		t.Fatal("want error for missing CA file")
	}
}

// TestCAFileIgnoredWhenCredentialsSet proves an explicit Credentials
// override takes precedence over CAFile, matching the documented contract.
func TestCAFileIgnoredWhenCredentialsSet(t *testing.T) {
	fake := &fakeUsers{}
	lis := startServer(t, fake)
	// A garbage CAFile would fail Dial if it were consulted; it must not be,
	// since Credentials (insecure, set by dialBuf) takes precedence.
	conn := dialBuf(t, lis, Config{Token: "t", CAFile: "/nonexistent/ca.pem"})

	ctx, _ := WithCallInfo(context.Background())
	if _, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
}
