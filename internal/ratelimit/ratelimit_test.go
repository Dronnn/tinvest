package ratelimit_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Dronnn/tinvest/internal/ratelimit"
	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

const getAccountsMethod = "/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"

type usersServer struct {
	investapi.UnimplementedUsersServiceServer
	tariff *investapi.GetUserTariffResponse
}

func (usersServer) GetAccounts(context.Context, *investapi.GetAccountsRequest) (*investapi.GetAccountsResponse, error) {
	return &investapi.GetAccountsResponse{}, nil
}

func (s usersServer) GetUserTariff(context.Context, *investapi.GetUserTariffRequest) (*investapi.GetUserTariffResponse, error) {
	if s.tariff != nil {
		return s.tariff, nil
	}
	return &investapi.GetUserTariffResponse{}, nil
}

func dial(t *testing.T, limiter *ratelimit.Limiter, servers ...usersServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	implementation := usersServer{}
	if len(servers) > 0 {
		implementation = servers[0]
	}
	investapi.RegisterUsersServiceServer(server, implementation)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithUnaryInterceptor(limiter.UnaryClientInterceptor()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestSuccessfulGetUserTariffRefreshesInterceptor(t *testing.T) {
	perSecond := int32(1)
	limiter := ratelimit.New([]ratelimit.Limit{{
		Group: "users", Services: []string{"UsersService"}, PerSecond: 100, Burst: 10,
	}}, 10*time.Millisecond)
	client := investapi.NewUsersServiceClient(dial(t, limiter, usersServer{tariff: &investapi.GetUserTariffResponse{
		UnaryLimits: []*investapi.UnaryLimit{{
			LimitPerSecond: &perSecond, Methods: []string{getAccountsMethod},
		}},
	}}))
	if _, err := client.GetUserTariff(context.Background(), &investapi.GetUserTariffRequest{}); err != nil {
		t.Fatalf("GetUserTariff: %v", err)
	}
	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("first GetAccounts: %v", err)
	}
	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err == nil {
		t.Fatal("want refreshed one-request burst to reject the second call")
	}
}

func TestBurstBlocksBrieflyThenSucceeds(t *testing.T) {
	limiter := ratelimit.New([]ratelimit.Limit{{
		Group: "users", Methods: []string{getAccountsMethod}, PerSecond: 20, Burst: 1,
	}}, 200*time.Millisecond)
	client := investapi.NewUsersServiceClient(dial(t, limiter))

	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("first GetAccounts: %v", err)
	}
	start := time.Now()
	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("second GetAccounts: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("second call waited %v, want token-bucket blocking near 50ms", elapsed)
	}
}

func TestDeadlineBoundFailureClassifiesAsExitFour(t *testing.T) {
	limiter := ratelimit.New([]ratelimit.Limit{{
		Group: "users", Methods: []string{getAccountsMethod}, PerSecond: 2, Burst: 1,
	}}, time.Second)
	client := investapi.NewUsersServiceClient(dial(t, limiter))

	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("first GetAccounts: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if err == nil {
		t.Fatal("want local rate-limit error")
	}
	classified := render.Classify(err, render.CallContext{Phase: transport.PhaseNotSent})
	if classified.Code != render.CodeRateLimited || classified.ExitCode() != render.ExitRateLimited {
		t.Fatalf("classified = %+v, want RATE_LIMITED exit 4", classified)
	}
}

func TestTariffRefreshReplacesStaticMethodGroup(t *testing.T) {
	limiter := ratelimit.New(ratelimit.DefaultLimits(), 100*time.Millisecond)
	limiter.RefreshTariff([]ratelimit.Limit{{
		Group: "tariff-0", Methods: []string{getAccountsMethod}, PerSecond: 100, Burst: 1,
	}})

	client := investapi.NewUsersServiceClient(dial(t, limiter))
	if _, err := client.GetAccounts(context.Background(), &investapi.GetAccountsRequest{}); err != nil {
		t.Fatalf("GetAccounts after tariff refresh: %v", err)
	}
}

func TestTariffRefreshRemovesStaleMethodMappings(t *testing.T) {
	limiter := ratelimit.New([]ratelimit.Limit{{
		Group: "users", Methods: []string{getAccountsMethod}, PerSecond: 100, Burst: 10,
	}}, 10*time.Millisecond)
	limiter.RefreshTariff([]ratelimit.Limit{{
		Group: "tariff-0", Methods: []string{getAccountsMethod}, PerSecond: 1, Burst: 1,
	}})
	if err := limiter.Wait(context.Background(), getAccountsMethod); err != nil {
		t.Fatalf("first tariff wait: %v", err)
	}
	if err := limiter.Wait(context.Background(), getAccountsMethod); err == nil {
		t.Fatal("want exhausted tariff bucket before refresh")
	}

	limiter.RefreshTariff([]ratelimit.Limit{{
		Group: "tariff-0", Methods: []string{"GetUserTariff"}, PerSecond: 1, Burst: 1,
	}})
	if err := limiter.Wait(context.Background(), getAccountsMethod); err != nil {
		t.Fatalf("stale tariff mapping survived refresh: %v", err)
	}
}

func TestFullMethodMappingsDoNotCollideAcrossServices(t *testing.T) {
	methodA := "/example.ServiceA/GetState"
	methodB := "/example.ServiceB/GetState"
	limiter := ratelimit.New([]ratelimit.Limit{
		{Group: "a", Methods: []string{methodA}, PerSecond: 1, Burst: 1},
		{Group: "b", Methods: []string{methodB}, PerSecond: 1, Burst: 1},
	}, 10*time.Millisecond)
	if err := limiter.Wait(context.Background(), methodA); err != nil {
		t.Fatalf("method A: %v", err)
	}
	if err := limiter.Wait(context.Background(), methodB); err != nil {
		t.Fatalf("method B incorrectly shared method A's bucket: %v", err)
	}
}
