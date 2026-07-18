package portfolio

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type fakeOperations struct {
	investapi.UnimplementedOperationsServiceServer

	accountIDs []string
	portfolio  *investapi.PortfolioResponse
	positions  *investapi.PositionsResponse
	limits     *investapi.WithdrawLimitsResponse
	err        error
}

func (f *fakeOperations) GetPortfolio(_ context.Context, req *investapi.PortfolioRequest) (*investapi.PortfolioResponse, error) {
	f.accountIDs = append(f.accountIDs, req.GetAccountId())
	if f.err != nil {
		return nil, f.err
	}
	return f.portfolio, nil
}

func (f *fakeOperations) GetPositions(_ context.Context, req *investapi.PositionsRequest) (*investapi.PositionsResponse, error) {
	f.accountIDs = append(f.accountIDs, req.GetAccountId())
	return f.positions, nil
}

func (f *fakeOperations) GetWithdrawLimits(_ context.Context, req *investapi.WithdrawLimitsRequest) (*investapi.WithdrawLimitsResponse, error) {
	f.accountIDs = append(f.accountIDs, req.GetAccountId())
	return f.limits, nil
}

func startOperationsServer(t *testing.T, fake *fakeOperations) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterOperationsServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestPortfolioPositionsAndWithdrawLimits(t *testing.T) {
	fake := &fakeOperations{
		portfolio: &investapi.PortfolioResponse{AccountId: "acc-1"},
		positions: &investapi.PositionsResponse{AccountId: "acc-1"},
		limits: &investapi.WithdrawLimitsResponse{Money: []*investapi.MoneyValue{
			{Currency: "rub", Units: 100},
		}},
	}
	client := New(startOperationsServer(t, fake))

	portfolio, err := client.Portfolio(context.Background(), "acc-1")
	if err != nil || portfolio.GetAccountId() != "acc-1" {
		t.Fatalf("Portfolio = %+v, %v", portfolio, err)
	}
	positions, err := client.Positions(context.Background(), "acc-1")
	if err != nil || positions.GetAccountId() != "acc-1" {
		t.Fatalf("Positions = %+v, %v", positions, err)
	}
	limits, err := client.WithdrawLimits(context.Background(), "acc-1")
	if err != nil || len(limits.GetMoney()) != 1 {
		t.Fatalf("WithdrawLimits = %+v, %v", limits, err)
	}
	if len(fake.accountIDs) != 3 {
		t.Fatalf("account IDs = %v", fake.accountIDs)
	}
	for _, got := range fake.accountIDs {
		if got != "acc-1" {
			t.Errorf("account ID = %q, want acc-1", got)
		}
	}
}

func TestPortfolioBrokerRejectionMapsToExitFive(t *testing.T) {
	fake := &fakeOperations{err: status.Error(codes.FailedPrecondition, "30001")}
	client := New(startOperationsServer(t, fake))

	ctx, info := transport.WithCallInfo(context.Background())
	_, err := client.Portfolio(ctx, "acc-1")
	if err == nil {
		t.Fatal("want broker error")
	}
	cerr := render.Classify(err, render.CallContext{Phase: info.Phase()})
	if cerr.ExitCode() != render.ExitRejected {
		t.Errorf("exit code = %d, want %d", cerr.ExitCode(), render.ExitRejected)
	}
}
