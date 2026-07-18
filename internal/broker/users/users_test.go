package users

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

type fakeUsers struct {
	investapi.UnimplementedUsersServiceServer

	tariff    *investapi.GetUserTariffResponse
	margin    *investapi.GetMarginAttributesResponse
	marginErr error
	accountID string
}

func (f *fakeUsers) GetUserTariff(context.Context, *investapi.GetUserTariffRequest) (*investapi.GetUserTariffResponse, error) {
	return f.tariff, nil
}

func (f *fakeUsers) GetMarginAttributes(_ context.Context, req *investapi.GetMarginAttributesRequest) (*investapi.GetMarginAttributesResponse, error) {
	f.accountID = req.GetAccountId()
	if f.marginErr != nil {
		return nil, f.marginErr
	}
	return f.margin, nil
}

func startUsersServer(t *testing.T, fake *fakeUsers) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterUsersServiceServer(srv, fake)
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

func TestTariffAndMargin(t *testing.T) {
	fake := &fakeUsers{
		tariff: &investapi.GetUserTariffResponse{UnaryLimits: []*investapi.UnaryLimit{{LimitPerMinute: 100}}},
		margin: &investapi.GetMarginAttributesResponse{LiquidPortfolio: &investapi.MoneyValue{Currency: "rub", Units: 1000}},
	}
	client := New(startUsersServer(t, fake))

	tariff, err := client.Tariff(context.Background())
	if err != nil || len(tariff.GetUnaryLimits()) != 1 {
		t.Fatalf("Tariff = %+v, %v", tariff, err)
	}
	margin, err := client.Margin(context.Background(), "acc-1")
	if err != nil || margin.GetLiquidPortfolio().GetUnits() != 1000 {
		t.Fatalf("Margin = %+v, %v", margin, err)
	}
	if fake.accountID != "acc-1" {
		t.Errorf("account ID = %q", fake.accountID)
	}
}

func TestMarginFailedPreconditionMapsToExitFive(t *testing.T) {
	fake := &fakeUsers{marginErr: status.Error(codes.FailedPrecondition, "not margin-enabled")}
	client := New(startUsersServer(t, fake))

	ctx, info := transport.WithCallInfo(context.Background())
	_, err := client.Margin(ctx, "acc-1")
	if err == nil {
		t.Fatal("want FAILED_PRECONDITION")
	}
	cerr := render.Classify(err, render.CallContext{Phase: info.Phase()})
	if cerr.Code != render.CodeBrokerRejected || cerr.ExitCode() != render.ExitRejected {
		t.Errorf("classified = %+v", cerr)
	}
}
