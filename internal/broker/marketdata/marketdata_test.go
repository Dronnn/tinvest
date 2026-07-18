package marketdata

import (
	"context"
	"net"
	"sync"
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

// fakeMarketData is an in-process MarketDataService capturing what the
// client sent and returning scripted responses or errors.
type fakeMarketData struct {
	investapi.UnimplementedMarketDataServiceServer

	mu sync.Mutex

	gotLastPricesIDs []string
	lastPricesErr    error
	lastPricesResp   []*investapi.LastPrice

	gotClosePriceIDs []string
	closePricesResp  []*investapi.InstrumentClosePriceResponse

	gotOrderBookReq *investapi.GetOrderBookRequest
	orderBookErr    error
	orderBookResp   *investapi.GetOrderBookResponse

	gotTradingStatusID string
	tradingStatusResp  *investapi.GetTradingStatusResponse
}

func (f *fakeMarketData) GetLastPrices(_ context.Context, req *investapi.GetLastPricesRequest) (*investapi.GetLastPricesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotLastPricesIDs = req.GetInstrumentId()
	if f.lastPricesErr != nil {
		return nil, f.lastPricesErr
	}
	return &investapi.GetLastPricesResponse{LastPrices: f.lastPricesResp}, nil
}

func (f *fakeMarketData) GetClosePrices(_ context.Context, req *investapi.GetClosePricesRequest) (*investapi.GetClosePricesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range req.GetInstruments() {
		f.gotClosePriceIDs = append(f.gotClosePriceIDs, r.GetInstrumentId())
	}
	return &investapi.GetClosePricesResponse{ClosePrices: f.closePricesResp}, nil
}

func (f *fakeMarketData) GetOrderBook(_ context.Context, req *investapi.GetOrderBookRequest) (*investapi.GetOrderBookResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotOrderBookReq = req
	if f.orderBookErr != nil {
		return nil, f.orderBookErr
	}
	return f.orderBookResp, nil
}

func (f *fakeMarketData) GetTradingStatus(_ context.Context, req *investapi.GetTradingStatusRequest) (*investapi.GetTradingStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotTradingStatusID = req.GetInstrumentId()
	return f.tradingStatusResp, nil
}

func startMarketDataServer(t *testing.T, f *fakeMarketData) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterMarketDataServiceServer(srv, f)
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

func TestLastPricesHappyPath(t *testing.T) {
	fake := &fakeMarketData{lastPricesResp: []*investapi.LastPrice{
		{InstrumentUid: "uid-1", Ticker: "SBER", Price: &investapi.Quotation{Units: 250, Nano: 0}},
	}}
	conn := startMarketDataServer(t, fake)

	got, err := New(conn).LastPrices(context.Background(), []string{"uid-1"})
	if err != nil {
		t.Fatalf("LastPrices: %v", err)
	}
	if len(got) != 1 || got[0].GetInstrumentUid() != "uid-1" {
		t.Fatalf("got %+v", got)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.gotLastPricesIDs) != 1 || fake.gotLastPricesIDs[0] != "uid-1" {
		t.Errorf("instrument_id sent = %v, want [uid-1]", fake.gotLastPricesIDs)
	}
}

func TestLastPricesNotFoundMapsToExitFive(t *testing.T) {
	fake := &fakeMarketData{lastPricesErr: status.Error(codes.NotFound, "50002")}
	conn := startMarketDataServer(t, fake)

	ctx, info := transport.WithCallInfo(context.Background())
	_, err := New(conn).LastPrices(ctx, []string{"missing-uid"})
	if err == nil {
		t.Fatal("want NOT_FOUND error")
	}
	cerr := render.Classify(err, render.CallContext{Phase: info.Phase()})
	if got := cerr.ExitCode(); got != render.ExitRejected {
		t.Errorf("exit code = %d, want %d", got, render.ExitRejected)
	}
}

func TestClosePricesHappyPath(t *testing.T) {
	fake := &fakeMarketData{closePricesResp: []*investapi.InstrumentClosePriceResponse{
		{InstrumentUid: "uid-1", Price: &investapi.Quotation{Units: 251, Nano: 500000000}},
	}}
	conn := startMarketDataServer(t, fake)

	got, err := New(conn).ClosePrices(context.Background(), []string{"uid-1", "uid-2"})
	if err != nil {
		t.Fatalf("ClosePrices: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d results, want 1", len(got))
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.gotClosePriceIDs) != 2 {
		t.Errorf("got %d instrument requests, want 2", len(fake.gotClosePriceIDs))
	}
}

func TestValidateDepth(t *testing.T) {
	for _, d := range ValidDepths {
		if err := ValidateDepth(d); err != nil {
			t.Errorf("ValidateDepth(%d): %v, want nil", d, err)
		}
	}
	for _, d := range []int32{0, -1, 2, 5, 11, 15, 60, 100} {
		if err := ValidateDepth(d); err == nil {
			t.Errorf("ValidateDepth(%d): want error", d)
		}
	}
}

func TestOrderBookRejectsInvalidDepthLocally(t *testing.T) {
	fake := &fakeMarketData{}
	conn := startMarketDataServer(t, fake)

	_, err := New(conn).OrderBook(context.Background(), "uid-1", 7)
	if err == nil {
		t.Fatal("want a local validation error for depth=7")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.gotOrderBookReq != nil {
		t.Error("invalid depth must never reach the broker")
	}
}

func TestOrderBookHappyPath(t *testing.T) {
	fake := &fakeMarketData{orderBookResp: &investapi.GetOrderBookResponse{
		InstrumentUid: "uid-1",
		Depth:         20,
		Bids:          []*investapi.Order{{Price: &investapi.Quotation{Units: 100}, Quantity: 5}},
		Asks:          []*investapi.Order{{Price: &investapi.Quotation{Units: 101}, Quantity: 3}},
	}}
	conn := startMarketDataServer(t, fake)

	got, err := New(conn).OrderBook(context.Background(), "uid-1", 20)
	if err != nil {
		t.Fatalf("OrderBook: %v", err)
	}
	if got.GetDepth() != 20 || len(got.GetBids()) != 1 || len(got.GetAsks()) != 1 {
		t.Fatalf("got %+v", got)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.gotOrderBookReq.GetInstrumentId() != "uid-1" {
		t.Errorf("instrument_id = %q, want uid-1", fake.gotOrderBookReq.GetInstrumentId())
	}
	if fake.gotOrderBookReq.GetDepth() != 20 {
		t.Errorf("depth = %d, want 20", fake.gotOrderBookReq.GetDepth())
	}
}

func TestOrderBookNotFoundMapsToExitFive(t *testing.T) {
	fake := &fakeMarketData{orderBookErr: status.Error(codes.NotFound, "50002")}
	conn := startMarketDataServer(t, fake)

	ctx, info := transport.WithCallInfo(context.Background())
	_, err := New(conn).OrderBook(ctx, "missing-uid", 20)
	if err == nil {
		t.Fatal("want NOT_FOUND error")
	}
	cerr := render.Classify(err, render.CallContext{Phase: info.Phase()})
	if got := cerr.ExitCode(); got != render.ExitRejected {
		t.Errorf("exit code = %d, want %d", got, render.ExitRejected)
	}
}

func TestTradingStatusHappyPath(t *testing.T) {
	fake := &fakeMarketData{tradingStatusResp: &investapi.GetTradingStatusResponse{
		InstrumentUid: "uid-1",
		TradingStatus: investapi.SecurityTradingStatus_SECURITY_TRADING_STATUS_NORMAL_TRADING,
	}}
	conn := startMarketDataServer(t, fake)

	got, err := New(conn).TradingStatus(context.Background(), "uid-1")
	if err != nil {
		t.Fatalf("TradingStatus: %v", err)
	}
	if got.GetTradingStatus() != investapi.SecurityTradingStatus_SECURITY_TRADING_STATUS_NORMAL_TRADING {
		t.Errorf("trading status = %v", got.GetTradingStatus())
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.gotTradingStatusID != "uid-1" {
		t.Errorf("instrument_id = %q, want uid-1", fake.gotTradingStatusID)
	}
}
