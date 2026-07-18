package transport

import (
	"context"
	"errors"
	"net"
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
