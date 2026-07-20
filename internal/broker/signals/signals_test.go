package signals

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type fakeSignals struct {
	investapi.UnimplementedSignalServiceServer

	strategies  []*investapi.Strategy
	signals     []*investapi.Signal
	signalPages [][]*investapi.Signal
	signalsErr  error
	strategyID  string
	pageNumbers []int32
	pageLimits  []int32
}

func (f *fakeSignals) GetStrategies(context.Context, *investapi.GetStrategiesRequest) (*investapi.GetStrategiesResponse, error) {
	return &investapi.GetStrategiesResponse{Strategies: f.strategies}, nil
}

func (f *fakeSignals) GetSignals(_ context.Context, req *investapi.GetSignalsRequest) (*investapi.GetSignalsResponse, error) {
	f.strategyID = req.GetStrategyId()
	f.pageNumbers = append(f.pageNumbers, req.GetPaging().GetPageNumber())
	f.pageLimits = append(f.pageLimits, req.GetPaging().GetLimit())
	if f.signalsErr != nil {
		return nil, f.signalsErr
	}
	pages := f.signalPages
	if len(pages) == 0 {
		pages = [][]*investapi.Signal{f.signals}
	}
	total := 0
	for _, page := range pages {
		total += len(page)
	}
	pageNumber := req.GetPaging().GetPageNumber()
	if pageNumber < 0 || int(pageNumber) >= len(pages) {
		return &investapi.GetSignalsResponse{Paging: &investapi.PageResponse{TotalCount: int32(total)}}, nil
	}
	return &investapi.GetSignalsResponse{
		Signals: pages[pageNumber],
		Paging: &investapi.PageResponse{
			Limit: req.GetPaging().GetLimit(), PageNumber: pageNumber, TotalCount: int32(total),
		},
	}, nil
}

func startSignalsServer(t *testing.T, fake *fakeSignals) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterSignalServiceServer(srv, fake)
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

func TestStrategiesAndSignals(t *testing.T) {
	fake := &fakeSignals{
		strategies: []*investapi.Strategy{{StrategyId: "strategy-1", StrategyName: "Momentum"}},
		signalPages: [][]*investapi.Signal{
			{{SignalId: "signal-1", StrategyId: "strategy-1", InstrumentUid: "uid-1"}},
			{{SignalId: "signal-2", StrategyId: "strategy-1", InstrumentUid: "uid-2"}},
		},
	}
	client := New(startSignalsServer(t, fake))

	strategies, err := client.Strategies(context.Background())
	if err != nil || len(strategies) != 1 {
		t.Fatalf("Strategies = %+v, %v", strategies, err)
	}
	signals, err := client.Signals(context.Background(), "strategy-1")
	if err != nil || len(signals) != 2 {
		t.Fatalf("Signals = %+v, %v", signals, err)
	}
	if fake.strategyID != "strategy-1" {
		t.Errorf("strategy ID = %q", fake.strategyID)
	}
	if len(fake.pageNumbers) != 2 || fake.pageNumbers[0] != 0 || fake.pageNumbers[1] != 1 {
		t.Errorf("page numbers = %v", fake.pageNumbers)
	}
	if fake.pageLimits[0] <= 0 || fake.pageLimits[0] != fake.pageLimits[1] {
		t.Errorf("page limits = %v", fake.pageLimits)
	}
}

func TestSignalsErrorPath(t *testing.T) {
	fake := &fakeSignals{signalsErr: status.Error(codes.InvalidArgument, "bad strategy")}
	client := New(startSignalsServer(t, fake))

	ctx, info := transport.WithCallInfo(context.Background())
	_, err := client.Signals(ctx, "bad")
	if err == nil {
		t.Fatal("want error")
	}
	cerr := render.Classify(err, render.CallContext{Phase: info.Phase()})
	if cerr.ExitCode() != render.ExitRejected {
		t.Errorf("exit code = %d, want %d", cerr.ExitCode(), render.ExitRejected)
	}
}
