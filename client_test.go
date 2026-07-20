package tinvest

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Dronnn/tinvest/internal/transport"
	"github.com/Dronnn/tinvest/internal/transport/retry"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// fakeServer is an in-process InstrumentsService + MarketDataService that
// captures what the client sent and returns scripted responses. It backs the
// facade tests the same way internal/broker's bufconn fakes back theirs.
type fakeServer struct {
	investapi.UnimplementedInstrumentsServiceServer
	investapi.UnimplementedMarketDataServiceServer

	mu sync.Mutex

	instrumentByCalls     int
	instrumentByFailFirst int         // return Unavailable this many times before succeeding
	instrumentByErr       error       // returned by GetInstrumentBy when set
	instrumentByTrailer   metadata.MD // trailer set on every GetInstrumentBy
	candlesTrailer        metadata.MD // trailer set on every GetCandles
	gotInstrumentReqs     []*investapi.InstrumentRequest
	gotAuthority          []string

	gotSearchQuery        string
	gotLastPriceIDs       []string
	gotOrderBookReq       *investapi.GetOrderBookRequest
	gotCandleReqs         []*investapi.GetCandlesRequest
	gotTradingStatusID    string
	gotDividendsReq       *investapi.GetDividendsRequest
	gotForecastID         string
	gotInsiderReq         *investapi.GetInsiderDealsRequest
	gotFundamentalsAssets []string
	gotNewsReq            *investapi.NewsRequest
}

func (f *fakeServer) forecastID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotForecastID
}

func (f *fakeServer) insiderInstrument() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotInsiderReq.GetInstrumentId()
}

func (f *fakeServer) fundamentalsAssets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotFundamentalsAssets
}

func (f *fakeServer) captureAuth(ctx context.Context) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		f.gotAuthority = append(f.gotAuthority, md.Get("authorization")...)
	}
}

func (f *fakeServer) GetInstrumentBy(ctx context.Context, in *investapi.InstrumentRequest) (*investapi.InstrumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.instrumentByCalls++
	f.gotInstrumentReqs = append(f.gotInstrumentReqs, in)
	f.captureAuth(ctx)
	if f.instrumentByTrailer != nil {
		_ = grpc.SetTrailer(ctx, f.instrumentByTrailer)
	}
	if f.instrumentByFailFirst > 0 {
		f.instrumentByFailFirst--
		return nil, status.Error(codes.Unavailable, "please retry")
	}
	if f.instrumentByErr != nil {
		return nil, f.instrumentByErr
	}
	return &investapi.InstrumentResponse{Instrument: &investapi.Instrument{
		Uid: "U-" + in.GetId(), Figi: in.GetId(), Ticker: in.GetId(), AssetUid: "A-" + in.GetId(),
	}}, nil
}

func (f *fakeServer) FindInstrument(_ context.Context, in *investapi.FindInstrumentRequest) (*investapi.FindInstrumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotSearchQuery = in.GetQuery()
	return &investapi.FindInstrumentResponse{Instruments: []*investapi.InstrumentShort{
		{Uid: "u1", Ticker: "SBER", Figi: "BBG004730N88"},
	}}, nil
}

func (f *fakeServer) Shares(_ context.Context, _ *investapi.InstrumentsRequest) (*investapi.SharesResponse, error) {
	return &investapi.SharesResponse{Instruments: []*investapi.Share{
		{Uid: "u1", Figi: "BBG004730N88", Ticker: "SBER", ClassCode: "TQBR", Name: "Sberbank", Lot: 10, Currency: "rub"},
	}}, nil
}

func (f *fakeServer) GetDividends(_ context.Context, in *investapi.GetDividendsRequest) (*investapi.GetDividendsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotDividendsReq = in
	return &investapi.GetDividendsResponse{Dividends: []*investapi.Dividend{
		{DividendType: "regular"},
	}}, nil
}

func (f *fakeServer) News(_ context.Context, in *investapi.NewsRequest) (*investapi.NewsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotNewsReq = in
	return &investapi.NewsResponse{Items: []*investapi.NewsItem{{Title: "headline"}}, HasNext: false}, nil
}

func (f *fakeServer) GetForecastBy(_ context.Context, in *investapi.GetForecastRequest) (*investapi.GetForecastResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotForecastID = in.GetInstrumentId()
	return &investapi.GetForecastResponse{
		Consensus: &investapi.GetForecastResponse_ConsensusItem{Ticker: "SBER"},
	}, nil
}

func (f *fakeServer) GetInsiderDeals(_ context.Context, in *investapi.GetInsiderDealsRequest) (*investapi.GetInsiderDealsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotInsiderReq = in
	return &investapi.GetInsiderDealsResponse{InsiderDeals: []*investapi.GetInsiderDealsResponse_InsiderDeal{{}}}, nil
}

func (f *fakeServer) GetAssetFundamentals(_ context.Context, in *investapi.GetAssetFundamentalsRequest) (*investapi.GetAssetFundamentalsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotFundamentalsAssets = in.GetAssets()
	return &investapi.GetAssetFundamentalsResponse{Fundamentals: []*investapi.GetAssetFundamentalsResponse_StatisticResponse{{AssetUid: "A-x"}}}, nil
}

func (f *fakeServer) GetLastPrices(_ context.Context, in *investapi.GetLastPricesRequest) (*investapi.GetLastPricesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotLastPriceIDs = in.GetInstrumentId()
	return &investapi.GetLastPricesResponse{LastPrices: []*investapi.LastPrice{
		{InstrumentUid: "u1", Price: &investapi.Quotation{Units: 250, Nano: 500000000}},
	}}, nil
}

func (f *fakeServer) GetOrderBook(_ context.Context, in *investapi.GetOrderBookRequest) (*investapi.GetOrderBookResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotOrderBookReq = in
	return &investapi.GetOrderBookResponse{Depth: in.GetDepth(), InstrumentUid: in.GetInstrumentId()}, nil
}

func (f *fakeServer) GetCandles(ctx context.Context, in *investapi.GetCandlesRequest) (*investapi.GetCandlesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotCandleReqs = append(f.gotCandleReqs, in)
	if f.candlesTrailer != nil {
		_ = grpc.SetTrailer(ctx, f.candlesTrailer)
	}
	return &investapi.GetCandlesResponse{Candles: []*investapi.HistoricCandle{{IsComplete: true}}}, nil
}

func (f *fakeServer) GetTradingStatus(_ context.Context, in *investapi.GetTradingStatusRequest) (*investapi.GetTradingStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotTradingStatusID = in.GetInstrumentId()
	return &investapi.GetTradingStatusResponse{InstrumentUid: in.GetInstrumentId()}, nil
}

// newFakeClient starts the fake over bufconn and returns a Client whose
// connection carries the interceptor stack New installs — Bearer auth,
// per-call deadline, and the default retry policy — so the domain tests
// exercise that wiring, not a bare connection. The rate limiter is covered
// directly in the clientconn package tests; it is left out here so per-group
// bucket state cannot make these tests nondeterministic.
func newFakeClient(t *testing.T, fake *fakeServer) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	serverOpt, clientCreds := bufTLS(t)
	srv := grpc.NewServer(serverOpt)
	investapi.RegisterInstrumentsServiceServer(srv, fake)
	investapi.RegisterMarketDataServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	policy := retry.DefaultRetryPolicy()
	conn, err := transport.Dial(context.Background(), transport.Config{
		Endpoint:    "passthrough:///bufnet",
		Token:       "test-token",
		Credentials: clientCreds,
		RetryPolicy: &policy,
	}, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := newClient(conn, nil)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

const testUID = "11111111-1111-4111-8111-111111111111"

func TestNewRequiresToken(t *testing.T) {
	if _, err := New(context.Background(), Config{Token: "  "}); err == nil {
		t.Fatal("expected an error for a blank token")
	}
}

func TestNewCAFileError(t *testing.T) {
	_, err := New(context.Background(), Config{Token: "tok", CAFile: "/no/such/ca-bundle.pem"})
	if err == nil {
		t.Fatal("expected an error for an unreadable CA file")
	}
}

func TestNewLazyConnectAndClose(t *testing.T) {
	// grpc.NewClient is lazy, so New succeeds against an unreachable endpoint
	// without any network traffic; Close must then succeed.
	client, err := New(context.Background(), Config{Token: "tok", Endpoint: "127.0.0.1:1", DisableCache: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestResolvePassesIdentifierType(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)

	inst, err := client.Resolve(context.Background(), testUID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if inst.GetUid() != "U-"+testUID {
		t.Fatalf("uid = %q, want %q", inst.GetUid(), "U-"+testUID)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.gotInstrumentReqs[0].GetIdType(); got != investapi.InstrumentIdType_INSTRUMENT_ID_TYPE_UID {
		t.Fatalf("id type = %v, want UID", got)
	}
	if len(fake.gotAuthority) == 0 || !strings.Contains(fake.gotAuthority[0], "Bearer test-token") {
		t.Fatalf("authorization metadata = %v, want a Bearer token", fake.gotAuthority)
	}
}

func TestResolveRejectsMalformedIDWithoutNetwork(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)

	if _, err := client.Resolve(context.Background(), "not-an-id"); err == nil {
		t.Fatal("expected an error for a malformed identifier")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.instrumentByCalls != 0 {
		t.Fatalf("GetInstrumentBy called %d times, want 0 (rejected locally)", fake.instrumentByCalls)
	}
}

func TestLastPricesResolvesToUID(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)

	figi := "BBG004730N88"
	if _, err := client.LastPrices(context.Background(), figi); err != nil {
		t.Fatalf("LastPrices: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	// The figi must have been resolved to its uid before the quote call.
	if len(fake.gotLastPriceIDs) != 1 || fake.gotLastPriceIDs[0] != "U-"+figi {
		t.Fatalf("GetLastPrices instrument_id = %v, want [U-%s]", fake.gotLastPriceIDs, figi)
	}
}

func TestOrderBookValidatesDepth(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)

	if _, err := client.OrderBook(context.Background(), testUID, 7); err == nil {
		t.Fatal("expected an error for an invalid depth")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.gotOrderBookReq != nil {
		t.Fatal("GetOrderBook should not be called for an invalid depth")
	}
}

func TestCandlesReturnsData(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(48 * time.Hour)
	candles, err := client.Candles(context.Background(), testUID, investapi.CandleInterval_CANDLE_INTERVAL_DAY, from, to)
	if err != nil {
		t.Fatalf("Candles: %v", err)
	}
	if len(candles) != 1 {
		t.Fatalf("candles = %d, want 1", len(candles))
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.gotCandleReqs) != 1 || fake.gotCandleReqs[0].GetInstrumentId() != "U-"+testUID {
		t.Fatalf("candle req instrument = %v", fake.gotCandleReqs)
	}
}

func TestTradingStatusResolves(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)
	if _, err := client.TradingStatus(context.Background(), testUID); err != nil {
		t.Fatalf("TradingStatus: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.gotTradingStatusID != "U-"+testUID {
		t.Fatalf("trading-status instrument = %q", fake.gotTradingStatusID)
	}
}

func TestDividendsResolves(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)
	divs, err := client.Dividends(context.Background(), testUID, from, to)
	if err != nil {
		t.Fatalf("Dividends: %v", err)
	}
	if len(divs) != 1 {
		t.Fatalf("dividends = %d, want 1", len(divs))
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.gotDividendsReq.GetInstrumentId() != "U-"+testUID {
		t.Fatalf("dividends instrument = %q", fake.gotDividendsReq.GetInstrumentId())
	}
}

func TestSearchAndList(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)

	found, err := client.Search(context.Background(), "sber")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(found) != 1 || found[0].GetTicker() != "SBER" {
		t.Fatalf("search result = %+v", found)
	}

	listed, err := client.Instruments(context.Background(), "share")
	if err != nil {
		t.Fatalf("Instruments: %v", err)
	}
	if len(listed) != 1 || listed[0].Ticker != "SBER" || listed[0].Type != "share" {
		t.Fatalf("list result = %+v", listed)
	}
}

func TestResearchDomain(t *testing.T) {
	fake := &fakeServer{}
	client := newFakeClient(t, fake)

	news, err := client.News(context.Background(), NewsParams{})
	if err != nil {
		t.Fatalf("News: %v", err)
	}
	if len(news.Items) != 1 {
		t.Fatalf("news items = %d, want 1", len(news.Items))
	}

	forecast, err := client.Forecast(context.Background(), testUID)
	if err != nil {
		t.Fatalf("Forecast: %v", err)
	}
	if forecast.Consensus.GetTicker() != "SBER" {
		t.Fatalf("forecast consensus = %+v", forecast.Consensus)
	}
	if got := fake.forecastID(); got != "U-"+testUID {
		t.Fatalf("forecast instrument = %q, want resolved uid", got)
	}

	deals, err := client.InsiderDeals(context.Background(), InsiderDealsParams{InstrumentID: testUID, Limit: 10})
	if err != nil {
		t.Fatalf("InsiderDeals: %v", err)
	}
	if len(deals.Deals) != 1 {
		t.Fatalf("insider deals = %d, want 1", len(deals.Deals))
	}
	if got := fake.insiderInstrument(); got != "U-"+testUID {
		t.Fatalf("insider instrument = %q, want resolved uid", got)
	}

	if _, err := client.Fundamentals(context.Background(), "A-1", "A-2"); err != nil {
		t.Fatalf("Fundamentals: %v", err)
	}
	if got := fake.fundamentalsAssets(); len(got) != 2 {
		t.Fatalf("fundamentals assets = %v, want 2", got)
	}
}

func TestRetryWiring(t *testing.T) {
	// Fail the first attempt with Unavailable; the default retry policy in the
	// facade's connection must retry and succeed, proving the retry
	// interceptor is present in the stack New installs.
	fake := &fakeServer{instrumentByFailFirst: 1}
	client := newFakeClient(t, fake)

	if _, err := client.Resolve(context.Background(), testUID); err != nil {
		t.Fatalf("Resolve should have succeeded after a retry: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.instrumentByCalls != 2 {
		t.Fatalf("GetInstrumentBy attempts = %d, want 2 (one failure + one retry)", fake.instrumentByCalls)
	}
}

func TestCandlesCancelledBetweenWindowsHasNoStaleTrackingID(t *testing.T) {
	// A 30h range at a 1-minute interval (24h broker cap) splits into two
	// windows with a fixed ~100ms pause between them. The context deadline
	// fires during that pause: window 1 succeeds and captures a tracking id,
	// window 2 never runs. The returned cancellation must NOT surface window 1's
	// tracking id.
	fake := &fakeServer{candlesTrailer: metadata.Pairs("x-tracking-id", "trk-window-1")}
	client := newFakeClient(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(30 * time.Hour)

	_, err := client.Candles(ctx, testUID, investapi.CandleInterval_CANDLE_INTERVAL_1_MIN, from, to)
	if err == nil {
		t.Fatal("expected a cancellation error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error %v is not an *APIError", err)
	}
	if apiErr.TrackingID != "" {
		t.Errorf("TrackingID = %q, want empty (cancelled between windows must not reuse window 1's id)", apiErr.TrackingID)
	}
}

func TestAPIErrorCarriesCodeAndTrackingID(t *testing.T) {
	fake := &fakeServer{
		instrumentByErr:     status.Error(codes.NotFound, "instrument not found"),
		instrumentByTrailer: metadata.Pairs("x-tracking-id", "track-abc-123"),
	}
	client := newFakeClient(t, fake)

	_, err := client.Resolve(context.Background(), testUID)
	if err == nil {
		t.Fatal("expected an error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error %v is not an *APIError", err)
	}
	if apiErr.GRPCCode != codes.NotFound {
		t.Errorf("GRPCCode = %s, want NotFound", apiErr.GRPCCode)
	}
	if apiErr.TrackingID != "track-abc-123" {
		t.Errorf("TrackingID = %q, want track-abc-123", apiErr.TrackingID)
	}
	// The underlying gRPC status is still reachable through Unwrap.
	if status.Code(err) != codes.NotFound {
		t.Errorf("status.Code(err) = %s, want NotFound", status.Code(err))
	}
}

func TestMoneyHelpers(t *testing.T) {
	if got := QuotationString(&investapi.Quotation{Units: 1, Nano: 500000000}); got != "1.5" {
		t.Fatalf("QuotationString = %q, want 1.5", got)
	}
	if got := MoneyString(&investapi.MoneyValue{Units: -2, Nano: -250000000, Currency: "usd"}); got != "-2.25" {
		t.Fatalf("MoneyString = %q, want -2.25", got)
	}
	if got := QuotationString(nil); got != "0" {
		t.Fatalf("QuotationString(nil) = %q, want 0", got)
	}
}
