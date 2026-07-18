package instruments

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	investapi "tinvest/internal/pb/investapi"
)

type fakeReferenceData struct {
	investapi.UnimplementedInstrumentsServiceServer

	mu       sync.Mutex
	statuses []investapi.InstrumentStatus
	err      error
	from     time.Time
	to       time.Time
	ids      []string
}

func (f *fakeReferenceData) recordList(req *investapi.InstrumentsRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, req.GetInstrumentStatus())
	return f.err
}

func (f *fakeReferenceData) Shares(_ context.Context, req *investapi.InstrumentsRequest) (*investapi.SharesResponse, error) {
	if err := f.recordList(req); err != nil {
		return nil, err
	}
	return &investapi.SharesResponse{Instruments: []*investapi.Share{{Uid: "share-1", Ticker: "SBER"}}}, nil
}

func (f *fakeReferenceData) Bonds(_ context.Context, req *investapi.InstrumentsRequest) (*investapi.BondsResponse, error) {
	if err := f.recordList(req); err != nil {
		return nil, err
	}
	return &investapi.BondsResponse{Instruments: []*investapi.Bond{{Uid: "bond-1", Ticker: "RU000A"}}}, nil
}

func (f *fakeReferenceData) Etfs(_ context.Context, req *investapi.InstrumentsRequest) (*investapi.EtfsResponse, error) {
	if err := f.recordList(req); err != nil {
		return nil, err
	}
	return &investapi.EtfsResponse{Instruments: []*investapi.Etf{{Uid: "etf-1", Ticker: "TMOS"}}}, nil
}

func (f *fakeReferenceData) Currencies(_ context.Context, req *investapi.InstrumentsRequest) (*investapi.CurrenciesResponse, error) {
	if err := f.recordList(req); err != nil {
		return nil, err
	}
	return &investapi.CurrenciesResponse{Instruments: []*investapi.Currency{{Uid: "currency-1", Ticker: "USD000UTSTOM"}}}, nil
}

func (f *fakeReferenceData) Futures(_ context.Context, req *investapi.InstrumentsRequest) (*investapi.FuturesResponse, error) {
	if err := f.recordList(req); err != nil {
		return nil, err
	}
	return &investapi.FuturesResponse{Instruments: []*investapi.Future{{Uid: "future-1", Ticker: "Si"}}}, nil
}

func (f *fakeReferenceData) Options(_ context.Context, req *investapi.InstrumentsRequest) (*investapi.OptionsResponse, error) {
	if err := f.recordList(req); err != nil {
		return nil, err
	}
	return &investapi.OptionsResponse{Instruments: []*investapi.Option{{Uid: "option-1", Ticker: "SBER-C"}}}, nil
}

func (f *fakeReferenceData) GetDividends(_ context.Context, req *investapi.GetDividendsRequest) (*investapi.GetDividendsResponse, error) {
	f.recordRange(req.GetInstrumentId(), req.GetFrom().AsTime(), req.GetTo().AsTime())
	return &investapi.GetDividendsResponse{Dividends: []*investapi.Dividend{{DividendNet: &investapi.MoneyValue{Currency: "rub", Units: 25}}}}, nil
}

func (f *fakeReferenceData) GetBondCoupons(_ context.Context, req *investapi.GetBondCouponsRequest) (*investapi.GetBondCouponsResponse, error) {
	f.recordRange(req.GetInstrumentId(), req.GetFrom().AsTime(), req.GetTo().AsTime())
	return &investapi.GetBondCouponsResponse{Events: []*investapi.Coupon{{Figi: "figi-1", CouponNumber: 2}}}, nil
}

func (f *fakeReferenceData) GetAccruedInterests(_ context.Context, req *investapi.GetAccruedInterestsRequest) (*investapi.GetAccruedInterestsResponse, error) {
	f.recordRange(req.GetInstrumentId(), req.GetFrom().AsTime(), req.GetTo().AsTime())
	return &investapi.GetAccruedInterestsResponse{AccruedInterests: []*investapi.AccruedInterest{{Value: &investapi.Quotation{Units: 3}}}}, nil
}

func (f *fakeReferenceData) TradingSchedules(_ context.Context, req *investapi.TradingSchedulesRequest) (*investapi.TradingSchedulesResponse, error) {
	f.recordRange(req.GetExchange(), req.GetFrom().AsTime(), req.GetTo().AsTime())
	return &investapi.TradingSchedulesResponse{Exchanges: []*investapi.TradingSchedule{{Exchange: "MOEX"}}}, nil
}

func (f *fakeReferenceData) recordRange(id string, from, to time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
	f.from = from
	f.to = to
}

func startReferenceServer(t *testing.T, fake *fakeReferenceData) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterInstrumentsServiceServer(srv, fake)
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

func TestListDispatchesEverySupportedTypeWithBaseStatus(t *testing.T) {
	fake := &fakeReferenceData{}
	client := New(startReferenceServer(t, fake), nil)
	types := []string{"share", "bond", "etf", "currency", "future", "option"}
	for _, instrumentType := range types {
		list, err := client.List(context.Background(), instrumentType)
		if err != nil {
			t.Fatalf("List(%s): %v", instrumentType, err)
		}
		if len(list) != 1 || list[0].Type != instrumentType || list[0].UID == "" {
			t.Errorf("List(%s) = %+v", instrumentType, list)
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.statuses) != len(types) {
		t.Fatalf("statuses = %v", fake.statuses)
	}
	for _, got := range fake.statuses {
		if got != investapi.InstrumentStatus_INSTRUMENT_STATUS_BASE {
			t.Errorf("status = %v, want BASE", got)
		}
	}
}

func TestInstrumentEventsAndSchedules(t *testing.T) {
	fake := &fakeReferenceData{}
	client := New(startReferenceServer(t, fake), nil)
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 1, 0)

	dividends, err := client.Dividends(context.Background(), "uid-1", from, to)
	if err != nil || len(dividends) != 1 {
		t.Fatalf("Dividends = %+v, %v", dividends, err)
	}
	coupons, err := client.Coupons(context.Background(), "uid-1", from, to)
	if err != nil || len(coupons) != 1 {
		t.Fatalf("Coupons = %+v, %v", coupons, err)
	}
	accrued, err := client.AccruedInterests(context.Background(), "uid-1", from, to)
	if err != nil || len(accrued) != 1 {
		t.Fatalf("AccruedInterests = %+v, %v", accrued, err)
	}
	schedules, err := client.Schedules(context.Background(), "MOEX", from, to)
	if err != nil || len(schedules) != 1 {
		t.Fatalf("Schedules = %+v, %v", schedules, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.ids) != 4 || fake.ids[0] != "uid-1" || fake.ids[3] != "MOEX" {
		t.Errorf("ids = %v", fake.ids)
	}
	if !fake.from.Equal(from) || !fake.to.Equal(to) {
		t.Errorf("range = %s...%s", fake.from, fake.to)
	}
}

func TestListErrorPath(t *testing.T) {
	fake := &fakeReferenceData{err: status.Error(codes.ResourceExhausted, "80002")}
	client := New(startReferenceServer(t, fake), nil)

	_, err := client.List(context.Background(), "share")
	if err == nil {
		t.Fatal("want error")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("code = %v, want %v", got, codes.ResourceExhausted)
	}
}

func TestListRejectsUnknownTypeLocally(t *testing.T) {
	fake := &fakeReferenceData{}
	client := New(startReferenceServer(t, fake), nil)
	if _, err := client.List(context.Background(), "crypto"); err == nil {
		t.Fatal("want unsupported type error")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.statuses) != 0 {
		t.Errorf("unexpected RPCs: %v", fake.statuses)
	}
}
