package e2e

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	investapi "tinvest/internal/pb/investapi"
)

// fakeServer is an in-process gRPC broker double. It implements enough of
// InstrumentsService (instrument resolution), OrdersService (placement,
// state, listing), and UsersService for the place/reconcile flows the suite
// exercises. Behavior is injected per test through onPostOrder/onGetState; the
// server records every request so tests can assert call counts and the exact
// order_id the CLI sent.
type fakeServer struct {
	investapi.UnimplementedOrdersServiceServer
	investapi.UnimplementedInstrumentsServiceServer
	investapi.UnimplementedUsersServiceServer

	grpcServer *grpc.Server
	port       string

	// Hooks are set before the CLI is launched and not mutated concurrently
	// with serving. onPostOrder/onGetState default to a filled order and a
	// NOT_FOUND lookup respectively when nil.
	onPostOrder func(context.Context, *investapi.PostOrderRequest) (*investapi.PostOrderResponse, error)
	onGetState  func(context.Context, *investapi.GetOrderStateRequest) (*investapi.OrderState, error)
	// onGetInstrument, when set, runs as a side effect during instrument
	// resolution (the response is unchanged). Used to simulate the operator
	// engaging the kill switch mid-flight, after the initial policy gate.
	onGetInstrument func()

	mu           sync.Mutex
	postOrders   []*investapi.PostOrderRequest
	instrLookups []*investapi.InstrumentRequest
	stateLookups []*investapi.GetOrderStateRequest
}

// newFakeServer starts a fresh TLS-secured fake on a loopback port and
// registers cleanup that force-stops it at test end.
func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split host/port: %v", err)
	}

	creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverCert}})
	s := grpc.NewServer(grpc.Creds(creds))
	f := &fakeServer{grpcServer: s, port: port}
	investapi.RegisterOrdersServiceServer(s, f)
	investapi.RegisterInstrumentsServiceServer(s, f)
	investapi.RegisterUsersServiceServer(s, f)

	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return f
}

// endpoint is the host:port to write into the profile. It reports the numeric
// loopback the listener actually binds (127.0.0.1), never "localhost". On macOS
// "localhost" resolves to [::1, 127.0.0.1] with IPv6 first, but the fake listens
// on 127.0.0.1 (IPv4) only, so a "localhost" target makes gRPC attempt ::1 first
// and fail over — measurably slow locally (tens of ms vs ~1ms), and on an
// IPv6-filtered or CPU-starved CI runner the ::1 attempt can hang all the way to
// the 10s per-call deadline (exit 6, NETWORK). A numeric IP target skips DNS and
// the dual-stack race entirely; TLS still verifies because the leaf carries a
// 127.0.0.1 IP SAN.
func (f *fakeServer) endpoint() string { return "127.0.0.1:" + f.port }

// setOnGetInstrument installs the resolution side-effect hook under the mutex,
// so it is synchronized with the server goroutine that reads it (the write must
// happen-before the handler's read, which the shared mutex guarantees).
func (f *fakeServer) setOnGetInstrument(fn func()) {
	f.mu.Lock()
	f.onGetInstrument = fn
	f.mu.Unlock()
}

// GetInstrumentBy echoes the requested id back as a fully-formed instrument so
// resolution, the resolved-policy checks, and the price-increment check all
// pass (min price increment is left nil, which skips the increment check).
func (f *fakeServer) GetInstrumentBy(_ context.Context, req *investapi.InstrumentRequest) (*investapi.InstrumentResponse, error) {
	f.mu.Lock()
	f.instrLookups = append(f.instrLookups, req)
	hook := f.onGetInstrument
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return &investapi.InstrumentResponse{Instrument: &investapi.Instrument{
		Uid:       req.GetId(),
		Figi:      "BBG000000TST",
		Ticker:    "TEST",
		ClassCode: "TQBR",
		Lot:       1,
		Currency:  "rub",
	}}, nil
}

func (f *fakeServer) PostOrder(ctx context.Context, req *investapi.PostOrderRequest) (*investapi.PostOrderResponse, error) {
	f.mu.Lock()
	f.postOrders = append(f.postOrders, req)
	f.mu.Unlock()
	if f.onPostOrder != nil {
		return f.onPostOrder(ctx, req)
	}
	return filledResponse(req), nil
}

func (f *fakeServer) GetOrderState(ctx context.Context, req *investapi.GetOrderStateRequest) (*investapi.OrderState, error) {
	f.mu.Lock()
	f.stateLookups = append(f.stateLookups, req)
	f.mu.Unlock()
	if f.onGetState != nil {
		return f.onGetState(ctx, req)
	}
	return nil, notFound()
}

// GetOrders answers the open-order cap read with an empty list; the suite never
// configures MaxOpenOrders, so this is only defensive.
func (f *fakeServer) GetOrders(context.Context, *investapi.GetOrdersRequest) (*investapi.GetOrdersResponse, error) {
	return &investapi.GetOrdersResponse{}, nil
}

// GetInfo satisfies the UsersService surface the task asks the fake to carry.
func (f *fakeServer) GetInfo(context.Context, *investapi.GetInfoRequest) (*investapi.GetInfoResponse, error) {
	return &investapi.GetInfoResponse{}, nil
}

// filledResponse is a clean, confirmed placement: a fully-filled limit order
// that drives placeExec to the broker-confirmed ledger stage.
func filledResponse(req *investapi.PostOrderRequest) *investapi.PostOrderResponse {
	return &investapi.PostOrderResponse{
		OrderId:               "exchange-" + req.GetOrderId(),
		ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL,
		LotsRequested:         req.GetQuantity(),
		LotsExecuted:          req.GetQuantity(),
		OrderRequestId:        req.GetOrderId(),
		InstrumentUid:         req.GetInstrumentId(),
		Direction:             req.GetDirection(),
		OrderType:             req.GetOrderType(),
	}
}

// --- recorded-call accessors (safe for concurrent readers) ---

func (f *fakeServer) postOrderRequests() []*investapi.PostOrderRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*investapi.PostOrderRequest, len(f.postOrders))
	copy(out, f.postOrders)
	return out
}

func (f *fakeServer) instrLookupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.instrLookups)
}

func (f *fakeServer) stateLookupRequests() []*investapi.GetOrderStateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*investapi.GetOrderStateRequest, len(f.stateLookups))
	copy(out, f.stateLookups)
	return out
}
