package retry

import (
	"context"
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

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// fakeUsers is an in-process UsersService whose GetAccounts (a recognized
// read RPC) and PayIn (a mutation) handlers are swappable per test.
type fakeUsers struct {
	investapi.UnimplementedUsersServiceServer

	mu                 sync.Mutex
	getAccountsHandler func(ctx context.Context) (*investapi.GetAccountsResponse, error)
	payInHandler       func(ctx context.Context) (*investapi.PayInResponse, error)
}

type localNoRetryError struct{}

func (localNoRetryError) Error() string { return "local limit" }
func (localNoRetryError) NoRetry() bool { return true }
func (localNoRetryError) GRPCStatus() *status.Status {
	return status.New(codes.ResourceExhausted, "local limit")
}

func (f *fakeUsers) GetAccounts(ctx context.Context, _ *investapi.GetAccountsRequest) (*investapi.GetAccountsResponse, error) {
	f.mu.Lock()
	h := f.getAccountsHandler
	f.mu.Unlock()
	if h != nil {
		return h(ctx)
	}
	return &investapi.GetAccountsResponse{}, nil
}

func (f *fakeUsers) PayIn(ctx context.Context, _ *investapi.PayInRequest) (*investapi.PayInResponse, error) {
	f.mu.Lock()
	h := f.payInHandler
	f.mu.Unlock()
	if h != nil {
		return h(ctx)
	}
	return &investapi.PayInResponse{}, nil
}

// dialWithInterceptor starts fakeUsers on an in-process bufconn listener and
// dials a client with only the given interceptor installed.
func dialWithInterceptor(t *testing.T, f *fakeUsers, interceptor grpc.UnaryClientInterceptor) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterUsersServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithChainUnaryInterceptor(interceptor),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestRetryOnUnavailableThenSuccess(t *testing.T) {
	var calls int32
	f := &fakeUsers{getAccountsHandler: func(context.Context) (*investapi.GetAccountsResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, status.Error(codes.Unavailable, "try again")
		}
		return &investapi.GetAccountsResponse{}, nil
	}}
	conn := dialWithInterceptor(t, f, NewUnaryClientInterceptor(DefaultRetryPolicy()))

	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(context.Background(), &investapi.GetAccountsRequest{})
	if err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2 (one retry after UNAVAILABLE)", got)
	}
}

func TestNoRetryOnInvalidArgument(t *testing.T) {
	var calls int32
	f := &fakeUsers{getAccountsHandler: func(context.Context) (*investapi.GetAccountsResponse, error) {
		atomic.AddInt32(&calls, 1)
		return nil, status.Error(codes.InvalidArgument, "bad request")
	}}
	conn := dialWithInterceptor(t, f, NewUnaryClientInterceptor(DefaultRetryPolicy()))

	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(context.Background(), &investapi.GetAccountsRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (INVALID_ARGUMENT is not retriable)", got)
	}
}

func TestResourceExhaustedHonorsZeroRateLimitReset(t *testing.T) {
	var calls int32
	f := &fakeUsers{getAccountsHandler: func(ctx context.Context) (*investapi.GetAccountsResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			_ = grpc.SetTrailer(ctx, metadata.Pairs("x-ratelimit-reset", "0"))
			return nil, status.Error(codes.ResourceExhausted, "slow down")
		}
		return &investapi.GetAccountsResponse{}, nil
	}}
	conn := dialWithInterceptor(t, f, NewUnaryClientInterceptor(DefaultRetryPolicy()))

	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(context.Background(), &investapi.GetAccountsRequest{})
	if err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2", got)
	}
}

func TestResourceExhaustedWaitsForReportedReset(t *testing.T) {
	var calls int32
	f := &fakeUsers{getAccountsHandler: func(ctx context.Context) (*investapi.GetAccountsResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			_ = grpc.SetTrailer(ctx, metadata.Pairs("x-ratelimit-reset", "1"))
			return nil, status.Error(codes.ResourceExhausted, "slow down")
		}
		return &investapi.GetAccountsResponse{}, nil
	}}
	conn := dialWithInterceptor(t, f, NewUnaryClientInterceptor(DefaultRetryPolicy()))

	start := time.Now()
	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(context.Background(), &investapi.GetAccountsRequest{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("GetAccounts: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2", got)
	}
	// jitter is +/-20% of the reported 1s => [800ms, 1200ms]; the default
	// exponential schedule's first-attempt wait tops out ~120ms, so this
	// range also proves the trailer value was honored rather than falling
	// back to the generic backoff.
	if elapsed < 700*time.Millisecond || elapsed > 1700*time.Millisecond {
		t.Errorf("elapsed = %v, want roughly the reported 1s reset (jittered)", elapsed)
	}
}

func TestResourceExhaustedNotRetriedWhenPolicyDisabled(t *testing.T) {
	var calls int32
	f := &fakeUsers{getAccountsHandler: func(context.Context) (*investapi.GetAccountsResponse, error) {
		atomic.AddInt32(&calls, 1)
		return nil, status.Error(codes.ResourceExhausted, "slow down")
	}}
	policy := RetryPolicy{MaxAttempts: 3, PerCallCodes: []codes.Code{codes.Unavailable}, RateLimitRetry: false}
	conn := dialWithInterceptor(t, f, NewUnaryClientInterceptor(policy))

	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(context.Background(), &investapi.GetAccountsRequest{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("err = %v, want ResourceExhausted", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (RateLimitRetry disabled)", got)
	}
}

func TestLocalNoRetryErrorStopsRetryLoop(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts: 3, PerCallCodes: []codes.Code{codes.ResourceExhausted}, RateLimitRetry: true,
	}
	interceptor := NewUnaryClientInterceptor(policy)
	calls := 0
	err := interceptor(
		context.Background(), investapi.UsersService_GetAccounts_FullMethodName,
		&investapi.GetAccountsRequest{}, &investapi.GetAccountsResponse{}, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			return localNoRetryError{}
		},
	)
	if status.Code(err) != codes.ResourceExhausted || calls != 1 {
		t.Fatalf("error = %v, calls = %d; want one local RATE_LIMITED result", err, calls)
	}
}

func TestMutationNotRetriedWithoutIdempotentMarker(t *testing.T) {
	var calls int32
	f := &fakeUsers{payInHandler: func(context.Context) (*investapi.PayInResponse, error) {
		atomic.AddInt32(&calls, 1)
		return nil, status.Error(codes.Unavailable, "try again")
	}}
	conn := dialWithInterceptor(t, f, NewUnaryClientInterceptor(DefaultRetryPolicy()))

	_, err := investapi.NewUsersServiceClient(conn).PayIn(context.Background(), &investapi.PayInRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (mutation without Idempotent must not retry)", got)
	}
}

func TestMutationRetriedWithIdempotentMarker(t *testing.T) {
	var calls int32
	f := &fakeUsers{payInHandler: func(context.Context) (*investapi.PayInResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, status.Error(codes.Unavailable, "try again")
		}
		return &investapi.PayInResponse{}, nil
	}}
	conn := dialWithInterceptor(t, f, NewUnaryClientInterceptor(DefaultRetryPolicy()))

	ctx := Idempotent(context.Background())
	_, err := investapi.NewUsersServiceClient(conn).PayIn(ctx, &investapi.PayInRequest{})
	if err != nil {
		t.Fatalf("PayIn: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2 (Idempotent opts the mutation into retry)", got)
	}
}

func TestIsReadMethod(t *testing.T) {
	cases := []struct {
		method string
		want   bool
	}{
		{"/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts", true},
		{"/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument", true},
		{"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares", true},
		{"/tinkoff.public.invest.api.contract.v1.InstrumentsService/ShareBy", true},
		{"/tinkoff.public.invest.api.contract.v1.OrdersService/PostOrder", false},
		{"/tinkoff.public.invest.api.contract.v1.UsersService/PayIn", false},
		{"/tinkoff.public.invest.api.contract.v1.OrdersService/CancelOrder", false},
	}
	for _, c := range cases {
		if got := isReadMethod(c.method); got != c.want {
			t.Errorf("isReadMethod(%q) = %v, want %v", c.method, got, c.want)
		}
	}
}

func TestExponentialJitterBackoffBounds(t *testing.T) {
	backoff := exponentialJitterBackoff(100*time.Millisecond, 5*time.Second, 0.20)
	cases := []struct {
		attempt  uint
		min, max time.Duration
	}{
		{1, 80 * time.Millisecond, 120 * time.Millisecond},
		{2, 160 * time.Millisecond, 240 * time.Millisecond},
		{3, 320 * time.Millisecond, 480 * time.Millisecond},
		{10, 4 * time.Second, 6 * time.Second}, // capped at 5s, then jittered
	}
	for _, c := range cases {
		for i := 0; i < 200; i++ {
			d := backoff(c.attempt)
			if d < c.min || d > c.max {
				t.Fatalf("attempt %d: backoff = %v, want [%v, %v]", c.attempt, d, c.min, c.max)
			}
		}
	}
}

func TestJitterUpVariesWithinBounds(t *testing.T) {
	base := time.Second
	seenBelow, seenAbove := false, false
	for i := 0; i < 500; i++ {
		d := jitterUp(base, 0.2)
		if d < 800*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("jitterUp = %v, out of [800ms, 1200ms]", d)
		}
		if d < base {
			seenBelow = true
		}
		if d > base {
			seenAbove = true
		}
	}
	if !seenBelow || !seenAbove {
		t.Error("jitterUp never varied both below and above the base across 500 samples")
	}
}
