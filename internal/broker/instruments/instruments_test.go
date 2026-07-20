package instruments

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// fakeInstruments is an in-process InstrumentsService capturing what the
// client sent and returning a scripted response or error.
type fakeInstruments struct {
	investapi.UnimplementedInstrumentsServiceServer

	mu           sync.Mutex
	gotRequests  []*investapi.InstrumentRequest
	gotFindQuery string
	resp         *investapi.Instrument
	err          error
	findResp     []*investapi.InstrumentShort
}

func (f *fakeInstruments) GetInstrumentBy(_ context.Context, req *investapi.InstrumentRequest) (*investapi.InstrumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotRequests = append(f.gotRequests, req)
	if f.err != nil {
		return nil, f.err
	}
	return &investapi.InstrumentResponse{Instrument: f.resp}, nil
}

func (f *fakeInstruments) FindInstrument(_ context.Context, req *investapi.FindInstrumentRequest) (*investapi.FindInstrumentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotFindQuery = req.GetQuery()
	return &investapi.FindInstrumentResponse{Instruments: f.findResp}, nil
}

func startInstrumentsServer(t *testing.T, f *fakeInstruments) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterInstrumentsServiceServer(srv, f)
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

func TestResolveByUID(t *testing.T) {
	uid := "e6123145-9665-43e0-8413-cd61b8aa9b13"
	fake := &fakeInstruments{resp: &investapi.Instrument{Uid: uid, Ticker: "SBER", ClassCode: "TQBR"}}
	conn := startInstrumentsServer(t, fake)

	inst, err := New(conn, nil).Resolve(context.Background(), uid, false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if inst.GetUid() != uid {
		t.Errorf("uid = %q, want %q", inst.GetUid(), uid)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.gotRequests) != 1 {
		t.Fatalf("got %d requests, want 1", len(fake.gotRequests))
	}
	req := fake.gotRequests[0]
	if req.GetIdType() != investapi.InstrumentIdType_INSTRUMENT_ID_TYPE_UID {
		t.Errorf("id_type = %v, want UID", req.GetIdType())
	}
	if req.GetId() != uid {
		t.Errorf("id = %q, want %q", req.GetId(), uid)
	}
}

func TestResolveByFIGI(t *testing.T) {
	fake := &fakeInstruments{resp: &investapi.Instrument{Uid: "uid-x", Figi: "BBG004730N88"}}
	conn := startInstrumentsServer(t, fake)

	inst, err := New(conn, nil).Resolve(context.Background(), "BBG004730N88", false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if inst.GetFigi() != "BBG004730N88" {
		t.Errorf("figi = %q", inst.GetFigi())
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	req := fake.gotRequests[0]
	if req.GetIdType() != investapi.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI {
		t.Errorf("id_type = %v, want FIGI", req.GetIdType())
	}
}

func TestResolveByTickerClassCode(t *testing.T) {
	fake := &fakeInstruments{resp: &investapi.Instrument{Uid: "uid-y", Ticker: "SBER", ClassCode: "TQBR"}}
	conn := startInstrumentsServer(t, fake)

	inst, err := New(conn, nil).Resolve(context.Background(), "SBER@TQBR", false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if inst.GetTicker() != "SBER" {
		t.Errorf("ticker = %q", inst.GetTicker())
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	req := fake.gotRequests[0]
	if req.GetIdType() != investapi.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER {
		t.Errorf("id_type = %v, want TICKER", req.GetIdType())
	}
	if req.GetId() != "SBER" {
		t.Errorf("id = %q, want SBER", req.GetId())
	}
	if req.GetClassCode() != "TQBR" {
		t.Errorf("class_code = %q, want TQBR", req.GetClassCode())
	}
}

func TestResolveInvalidIDNeverHitsTheNetwork(t *testing.T) {
	fake := &fakeInstruments{}
	conn := startInstrumentsServer(t, fake)

	_, err := New(conn, nil).Resolve(context.Background(), "garbage", false)
	if err == nil {
		t.Fatal("want error for garbage input")
	}
	var invalid *InvalidIDError
	if !errors.As(err, &invalid) {
		t.Errorf("error type = %T, want *InvalidIDError", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.gotRequests) != 0 {
		t.Errorf("got %d requests, want 0 — invalid input must never reach the broker", len(fake.gotRequests))
	}
}

func TestResolveReturnsNotFound(t *testing.T) {
	fake := &fakeInstruments{err: status.Error(codes.NotFound, "50002")}
	conn := startInstrumentsServer(t, fake)

	_, err := New(conn, nil).Resolve(context.Background(), "BBG004730N88", false)
	if err == nil {
		t.Fatal("want NOT_FOUND error")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %v, want %v", got, codes.NotFound)
	}
}

func TestResolveCachesOnlySuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instruments.json")
	cache := NewCache(path, 24*time.Hour, nil)

	fake := &fakeInstruments{err: status.Error(codes.NotFound, "50002")}
	conn := startInstrumentsServer(t, fake)

	client := New(conn, cache)
	if _, err := client.Resolve(context.Background(), "BBG004730N88", false); err == nil {
		t.Fatal("want error from the fake server")
	}
	if _, ok := cache.Get("BBG004730N88"); ok {
		t.Fatal("a failed resolution must never be cached")
	}

	fake.mu.Lock()
	fake.err = nil
	fake.resp = &investapi.Instrument{Uid: "uid-z", Figi: "BBG004730N88"}
	fake.mu.Unlock()

	inst, err := client.Resolve(context.Background(), "BBG004730N88", false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if inst.GetUid() != "uid-z" {
		t.Fatalf("uid = %q, want uid-z", inst.GetUid())
	}
	cached, ok := cache.Get("BBG004730N88")
	if !ok {
		t.Fatal("a successful resolution must be cached")
	}
	if cached.GetUid() != "uid-z" {
		t.Errorf("cached uid = %q, want uid-z", cached.GetUid())
	}
}

func TestResolveNoCacheBypassesReadAndWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instruments.json")
	cache := NewCache(path, 24*time.Hour, nil)
	if err := cache.Put("BBG004730N88", &investapi.Instrument{Uid: "stale-uid", Figi: "BBG004730N88"}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	fake := &fakeInstruments{resp: &investapi.Instrument{Uid: "fresh-uid", Figi: "BBG004730N88"}}
	conn := startInstrumentsServer(t, fake)

	inst, err := New(conn, cache).Resolve(context.Background(), "BBG004730N88", true)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if inst.GetUid() != "fresh-uid" {
		t.Errorf("uid = %q, want fresh-uid (cache should have been bypassed)", inst.GetUid())
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.gotRequests) != 1 {
		t.Errorf("got %d requests, want 1 — --no-cache must skip the cache read", len(fake.gotRequests))
	}

	// The stale entry must be untouched: --no-cache also skips the write.
	cached, ok := cache.Get("BBG004730N88")
	if !ok || cached.GetUid() != "stale-uid" {
		t.Errorf("cache entry changed by a --no-cache resolution: %+v, ok=%v", cached, ok)
	}
}

func TestResolveRefetchesCacheEntryThatContradictsLookupKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instruments.json")
	cache := NewCache(path, 24*time.Hour, nil)
	if err := cache.Put("SBER@TQBR", &investapi.Instrument{
		Uid: "poisoned-uid", Ticker: "GAZP", ClassCode: "TQBR",
	}); err != nil {
		t.Fatalf("seed poisoned cache: %v", err)
	}

	fresh := &investapi.Instrument{Uid: "fresh-uid", Ticker: "SBER", ClassCode: "TQBR"}
	fake := &fakeInstruments{resp: fresh}
	conn := startInstrumentsServer(t, fake)

	got, err := New(conn, cache).Resolve(context.Background(), "SBER@TQBR", false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.GetUid() != "fresh-uid" {
		t.Fatalf("Resolve returned uid %q from a contradictory cache entry; want fresh-uid", got.GetUid())
	}
	fake.mu.Lock()
	requests := len(fake.gotRequests)
	fake.mu.Unlock()
	if requests != 1 {
		t.Fatalf("broker requests = %d, want 1 refetch after rejecting poisoned cache", requests)
	}
}

func TestFind(t *testing.T) {
	fake := &fakeInstruments{findResp: []*investapi.InstrumentShort{
		{Uid: "uid-1", Ticker: "SBER"},
		{Uid: "uid-2", Ticker: "SBERP"},
	}}
	conn := startInstrumentsServer(t, fake)

	got, err := New(conn, nil).Find(context.Background(), "sber")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.gotFindQuery != "sber" {
		t.Errorf("query = %q, want sber", fake.gotFindQuery)
	}
}
