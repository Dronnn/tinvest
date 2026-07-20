package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Dronnn/tinvest/internal/broker/orders"
	"github.com/Dronnn/tinvest/internal/config"
	"github.com/Dronnn/tinvest/internal/ledger"
	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
	"github.com/Dronnn/tinvest/internal/transport/retry"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// fakeOrders is an in-process OrdersService scripting the behaviors the place
// flow tests need: block-forever (exit-7), transient UNAVAILABLE (retry),
// rejection, and order-state lookups (reconcile / wait).
type fakeOrders struct {
	investapi.UnimplementedOrdersServiceServer

	mu sync.Mutex

	postOrderIDs    []string // client order_id seen on each PostOrder attempt
	unavailableLeft int
	postResp        *investapi.PostOrderResponse
	postErr         error
	block           chan struct{} // when non-nil, PostOrder blocks until closed
	received        chan struct{} // when non-nil, closed once PostOrder is entered

	stateByRequest map[string]*investapi.OrderState // keyed by client order_id
	stateResp      *investapi.OrderState            // fallback for any lookup
	stateErr       error
	stateCalls     int
	stateReplies   []orderStateReply

	openOrders      []*investapi.OrderState
	listErr         error
	getOrdersReqs   []*investapi.GetOrdersRequest
	todayOnlyOrders []*investapi.OrderState // returned only when advanced_filters is set (ListToday)
	cancelReplies   []cancelReply
	cancelCalls     int

	previewResp *investapi.GetOrderPriceResponse
	maxLotsResp *investapi.GetMaxLotsResponse
}

type orderStateReply struct {
	state *investapi.OrderState
	err   error
}

type cancelReply struct {
	response *investapi.CancelOrderResponse
	err      error
}

func (f *fakeOrders) PostOrder(ctx context.Context, req *investapi.PostOrderRequest) (*investapi.PostOrderResponse, error) {
	f.mu.Lock()
	f.postOrderIDs = append(f.postOrderIDs, req.GetOrderId())
	if f.received != nil {
		close(f.received)
		f.received = nil
	}
	block := f.block
	if f.unavailableLeft > 0 {
		f.unavailableLeft--
		f.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "transient")
	}
	resp, err := f.postResp, f.postErr
	f.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
		}
		// Return a context error so the client surfaces the deadline; but the
		// client's own deadline usually fires first.
		return nil, status.Error(codes.DeadlineExceeded, "blocked")
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (f *fakeOrders) GetOrderState(_ context.Context, req *investapi.GetOrderStateRequest) (*investapi.OrderState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stateCalls++
	if len(f.stateReplies) > 0 {
		reply := f.stateReplies[0]
		f.stateReplies = f.stateReplies[1:]
		return reply.state, reply.err
	}
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	if s, ok := f.stateByRequest[req.GetOrderId()]; ok {
		return s, nil
	}
	if f.stateResp != nil {
		return f.stateResp, nil
	}
	return nil, status.Error(codes.NotFound, "70001")
}

func (f *fakeOrders) CancelOrder(_ context.Context, _ *investapi.CancelOrderRequest) (*investapi.CancelOrderResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls++
	if len(f.cancelReplies) == 0 {
		return nil, status.Error(codes.NotFound, "not found")
	}
	reply := f.cancelReplies[0]
	f.cancelReplies = f.cancelReplies[1:]
	return reply.response, reply.err
}

func TestOrdersReconcileSkipsForeignKindsAndReportsThem(t *testing.T) {
	led := testLedger(t)
	entry, err := led.Begin(ledger.Intent{
		IntentID: "foreign-stop", Kind: kindStopOrderPlace, AccountID: "acc-1",
		Profile: "test", OrderID: testUUID,
		Payload: map[string]any{"endpoint": testEndpoint},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}

	fake := &fakeOrders{}
	conn := newOrdersConn(t, fake)
	outcomes, cerr := reconcileFlowForTarget(
		context.Background(), orders.New(conn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
		reconcileOptions{SyncNotFoundDelay: 0},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "foreign" || !strings.Contains(outcomes[0].Error, stopReconcileCommand) {
		t.Fatalf("foreign outcome = %+v", outcomes)
	}
	fake.mu.Lock()
	stateCalls := fake.stateCalls
	fake.mu.Unlock()
	if stateCalls != 0 {
		t.Fatalf("GetOrderState calls = %d, want 0 for foreign intent", stateCalls)
	}
	if unresolved, _ := led.Unresolved(); len(unresolved) != 1 {
		t.Fatalf("foreign intent was resolved, unresolved = %d", len(unresolved))
	}
	data := newReconcileData(outcomes, stopReconcileCommand)
	if data.ForeignIntentCount != 1 || data.ForeignIntentHint != stopReconcileCommand {
		t.Fatalf("foreign summary = %+v", data)
	}
}

func TestOrdersReconcileSkipsMismatchedProfileAndEndpoint(t *testing.T) {
	tests := []struct {
		name           string
		profile        string
		endpoint       string
		activeProfile  string
		activeEndpoint string
		want           string
	}{
		{
			name: "profile", profile: "sandbox", endpoint: "sandbox.example:443",
			activeProfile: "prod", activeEndpoint: "prod.example:443", want: "--profile sandbox",
		},
		{
			name: "endpoint", profile: "main", endpoint: "sandbox.example:443",
			activeProfile: "main", activeEndpoint: "prod.example:443", want: "sandbox.example:443",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			led := testLedger(t)
			entry, err := led.Begin(ledger.Intent{
				IntentID: "mismatch-" + tt.name, Kind: kindOrderPlace, AccountID: "acc-1",
				Profile: tt.profile, OrderID: testUUID,
				Payload: map[string]any{"endpoint": tt.endpoint, "async": false},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := entry.SendStarted(); err != nil {
				t.Fatal(err)
			}
			fake := &fakeOrders{}
			conn := newOrdersConn(t, fake)
			outcomes, cerr := reconcileFlowForTarget(
				context.Background(), orders.New(conn), led,
				reconcileTarget{Profile: tt.activeProfile, Endpoint: tt.activeEndpoint},
				reconcileOptions{SyncNotFoundDelay: 0},
			)
			if cerr != nil {
				t.Fatalf("reconcile: %+v", cerr)
			}
			if len(outcomes) != 1 || outcomes[0].Outcome != "profile-mismatch" || !strings.Contains(outcomes[0].Error, tt.want) {
				t.Fatalf("mismatch outcome = %+v", outcomes)
			}
			fake.mu.Lock()
			stateCalls := fake.stateCalls
			fake.mu.Unlock()
			if stateCalls != 0 {
				t.Fatalf("GetOrderState calls = %d, want 0", stateCalls)
			}
			if unresolved, _ := led.Unresolved(); len(unresolved) != 1 {
				t.Fatalf("mismatched intent was resolved, unresolved = %d", len(unresolved))
			}
		})
	}
}

func TestOrdersReconcileLeavesLegacyEndpointlessIntentIndeterminate(t *testing.T) {
	led := testLedger(t)
	entry, err := led.Begin(ledger.Intent{
		IntentID: "legacy", Kind: kindOrderPlace, AccountID: "acc-1",
		Profile: "test", OrderID: testUUID, Payload: map[string]any{"async": false},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeOrders{}
	conn := newOrdersConn(t, fake)
	outcomes, cerr := reconcileFlowForTarget(
		context.Background(), orders.New(conn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
		reconcileOptions{SyncNotFoundDelay: 0},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "indeterminate" || !strings.Contains(outcomes[0].Error, "endpoint") {
		t.Fatalf("legacy outcome = %+v", outcomes)
	}
	if unresolved, _ := led.Unresolved(); len(unresolved) != 1 {
		t.Fatalf("legacy intent was resolved, unresolved = %d", len(unresolved))
	}
}

// TestPlace30057DuplicateReportNotFoundStaysUnresolved: when PostOrder returns
// error 30057 (duplicate order, original report missing), the intent must NOT be
// closed as rejected — the order may exist. placeExec must surface exit 7 with a
// reconcile hint and leave the intent unresolved so reconcile resolves it without
// placing a duplicate.
func TestPlace30057DuplicateReportNotFoundStaysUnresolved(t *testing.T) {
	fake := &fakeOrders{postErr: status.Error(codes.InvalidArgument, "30057")}
	conn := newOrdersConn(t, fake)
	led := testLedger(t)

	intent, params := placeIntent("order-dup")
	_, cerr := placeExec(context.Background(), orders.New(conn), led, intent, params, false)
	if cerr == nil {
		t.Fatal("want an error for 30057")
	}
	if cerr.Code != render.CodeUnconfirmed || cerr.ExitCode() != render.ExitUnconfirmed {
		t.Fatalf("code = %s exit = %d, want UNCONFIRMED / 7", cerr.Code, cerr.ExitCode())
	}
	if cerr.ReconcileHint == nil || cerr.ReconcileHint.OrderID != "order-dup" {
		t.Fatalf("want a reconcile hint carrying the order_id, got %+v", cerr.ReconcileHint)
	}
	unresolved, err := led.Unresolved()
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) != 1 || unresolved[0].IntentID() != "order-dup" {
		t.Fatalf("30057 intent must remain unresolved (not rejected), got %d entries", len(unresolved))
	}
}

// TestAsyncReconcileNeverClosesNotPlacedOnNotFound: a PostOrderAsync intent that
// reads NOT_FOUND from GetOrderState must never be closed as not-placed. It is
// resolved from the day's order list by order_request_id, or left unresolved for
// a later run. Async does a single GetOrderState (no 2s double-check).
func TestAsyncReconcileNeverClosesNotPlacedOnNotFound(t *testing.T) {
	tests := []struct {
		name           string
		openOrders     []*investapi.OrderState
		listErr        error
		wantOutcome    string
		wantUnresolved int
	}{
		{
			// A fast-filled async order is terminal, so it appears only in the
			// terminal-inclusive day list (ListToday) — the active-only List that
			// the code used before would miss it and never converge (F4).
			name: "fast-filled async converges via the terminal day list",
			openOrders: []*investapi.OrderState{
				{OrderId: "exch-async-1", OrderRequestId: testUUID, ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL},
			},
			wantOutcome: "placed", wantUnresolved: 0,
		},
		{
			name:        "absent from the list stays unresolved",
			wantOutcome: "unresolved", wantUnresolved: 1,
		},
		{
			name:        "order-list lookup failure is indeterminate",
			listErr:     status.Error(codes.PermissionDenied, "cannot confirm"),
			wantOutcome: "indeterminate", wantUnresolved: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			led := testLedger(t)
			entry, err := led.Begin(ledger.Intent{
				IntentID: "async-" + tt.name, Kind: kindOrderPlace, AccountID: "acc-1",
				Profile: "test", OrderID: testUUID,
				Payload: map[string]any{"endpoint": testEndpoint, "async": true, "created_at": time.Now().UTC().Format(time.RFC3339)},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := entry.SendStarted(); err != nil {
				t.Fatal(err)
			}
			fake := &fakeOrders{
				stateReplies: []orderStateReply{{err: status.Error(codes.NotFound, "not propagated")}},
				openOrders:   tt.openOrders,
				listErr:      tt.listErr,
			}
			conn := newOrdersConn(t, fake)
			outcomes, cerr := reconcileFlowForTarget(
				context.Background(), orders.New(conn), led,
				reconcileTarget{Profile: "test", Endpoint: testEndpoint},
				reconcileOptions{SyncNotFoundDelay: 0},
			)
			if cerr != nil {
				t.Fatalf("reconcile: %+v", cerr)
			}
			if len(outcomes) != 1 || outcomes[0].Outcome != tt.wantOutcome {
				t.Fatalf("outcomes = %+v, want %s", outcomes, tt.wantOutcome)
			}
			if outcomes[0].Outcome == "not-placed" {
				t.Fatal("async intent must never be closed as not-placed on NOT_FOUND")
			}
			fake.mu.Lock()
			stateCalls := fake.stateCalls
			reqs := fake.getOrdersReqs
			fake.mu.Unlock()
			if stateCalls != 1 {
				t.Fatalf("GetOrderState calls = %d, want 1 (async must not double-check)", stateCalls)
			}
			if len(reqs) != 1 || reqs[0].GetAdvancedFilters() == nil || len(reqs[0].GetAdvancedFilters().GetExecutionStatus()) == 0 {
				t.Fatalf("GetOrders reqs = %+v, want one terminal-inclusive ListToday call (advanced_filters.execution_status set)", reqs)
			}
			if unresolved, _ := led.Unresolved(); len(unresolved) != tt.wantUnresolved {
				t.Fatalf("unresolved = %d, want %d", len(unresolved), tt.wantUnresolved)
			}
		})
	}
}

// TestSyncReconcileRechecksNotFoundBeforeClosing: a synchronous PostOrder intent
// re-checks a NOT_FOUND once (after the delay) before deciding, since NOT_FOUND
// for a synchronous order is meaningful.
func TestSyncReconcileRechecksNotFoundBeforeClosing(t *testing.T) {
	today := time.Now().UTC().Format(time.RFC3339)
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format(time.RFC3339)
	tests := []struct {
		name           string
		replies        []orderStateReply
		createdAt      string
		dayList        []*investapi.OrderState // ListToday result (terminal-inclusive)
		wantOutcome    string
		wantUnresolved int
	}{
		{
			name: "appears on the recheck",
			replies: []orderStateReply{
				{err: status.Error(codes.NotFound, "not propagated")},
				{state: &investapi.OrderState{OrderId: "exchange-1", ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW}},
			},
			createdAt:   today,
			wantOutcome: "placed", wantUnresolved: 0,
		},
		{
			name: "two not found, today, absent from day list -> not-placed",
			replies: []orderStateReply{
				{err: status.Error(codes.NotFound, "first")},
				{err: status.Error(codes.NotFound, "second")},
			},
			createdAt:   today,
			wantOutcome: "not-placed", wantUnresolved: 0,
		},
		{
			name: "two not found, but found terminal in day list -> placed",
			replies: []orderStateReply{
				{err: status.Error(codes.NotFound, "first")},
				{err: status.Error(codes.NotFound, "second")},
			},
			createdAt: today,
			dayList: []*investapi.OrderState{{
				OrderId: "exch-term", OrderRequestId: testUUID,
				ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED,
			}},
			wantOutcome: "placed", wantUnresolved: 0,
		},
		{
			name: "two not found, intent predates today -> unresolved",
			replies: []orderStateReply{
				{err: status.Error(codes.NotFound, "first")},
				{err: status.Error(codes.NotFound, "second")},
			},
			createdAt:   yesterday,
			wantOutcome: "unresolved", wantUnresolved: 1,
		},
		{
			name: "single not found then inconclusive error",
			replies: []orderStateReply{
				{err: status.Error(codes.NotFound, "first")},
				{err: status.Error(codes.PermissionDenied, "cannot confirm")},
			},
			createdAt:   today,
			wantOutcome: "indeterminate", wantUnresolved: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			led := testLedger(t)
			entry, err := led.Begin(ledger.Intent{
				IntentID: "sync-" + tt.name, Kind: kindOrderPlace, AccountID: "acc-1",
				Profile: "test", OrderID: testUUID,
				Payload: map[string]any{"endpoint": testEndpoint, "async": false, "created_at": tt.createdAt},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := entry.SendStarted(); err != nil {
				t.Fatal(err)
			}
			fake := &fakeOrders{stateReplies: append([]orderStateReply(nil), tt.replies...), todayOnlyOrders: tt.dayList}
			conn := newOrdersConn(t, fake)
			outcomes, cerr := reconcileFlowForTarget(
				context.Background(), orders.New(conn), led,
				reconcileTarget{Profile: "test", Endpoint: testEndpoint},
				reconcileOptions{SyncNotFoundDelay: 0},
			)
			if cerr != nil {
				t.Fatalf("reconcile: %+v", cerr)
			}
			if len(outcomes) != 1 || outcomes[0].Outcome != tt.wantOutcome {
				t.Fatalf("outcomes = %+v, want %s", outcomes, tt.wantOutcome)
			}
			fake.mu.Lock()
			stateCalls := fake.stateCalls
			fake.mu.Unlock()
			if stateCalls != 2 {
				t.Fatalf("GetOrderState calls = %d, want 2 (sync double-check)", stateCalls)
			}
			if unresolved, _ := led.Unresolved(); len(unresolved) != tt.wantUnresolved {
				t.Fatalf("unresolved = %d, want %d", len(unresolved), tt.wantUnresolved)
			}
		})
	}
}

func (f *fakeOrders) GetOrders(_ context.Context, req *investapi.GetOrdersRequest) (*investapi.GetOrdersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getOrdersReqs = append(f.getOrdersReqs, req)
	if f.listErr != nil {
		return nil, f.listErr
	}
	// ListToday sets advanced_filters (terminal-inclusive). When the test wants a
	// distinct day-list result it sets todayOnlyOrders; otherwise both List and
	// ListToday see openOrders.
	if req.GetAdvancedFilters() != nil && f.todayOnlyOrders != nil {
		return &investapi.GetOrdersResponse{Orders: f.todayOnlyOrders}, nil
	}
	return &investapi.GetOrdersResponse{Orders: f.openOrders}, nil
}

func (f *fakeOrders) GetOrderPrice(_ context.Context, _ *investapi.GetOrderPriceRequest) (*investapi.GetOrderPriceResponse, error) {
	return f.previewResp, nil
}

func (f *fakeOrders) GetMaxLots(_ context.Context, _ *investapi.GetMaxLotsRequest) (*investapi.GetMaxLotsResponse, error) {
	return f.maxLotsResp, nil
}

// newOrdersConn dials the fake over bufconn with the default retry policy
// installed (so idempotent retries are exercised), mirroring the real connect().
func newOrdersConn(t *testing.T, f *fakeOrders) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	serverOpt, clientCreds := bufTLS(t)
	srv := grpc.NewServer(serverOpt)
	investapi.RegisterOrdersServiceServer(srv, f)
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
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func testLedger(t *testing.T) *ledger.Ledger {
	t.Helper()
	led, err := ledger.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	t.Cleanup(func() { _ = led.Close() })
	return led
}

func placeIntent(orderID string) (ledger.Intent, orders.PlaceParams) {
	intent := ledger.Intent{
		IntentID: orderID, Kind: kindOrderPlace, AccountID: "acc-1",
		Profile: "test", Attempt: 1, OrderID: orderID,
		Payload: orderPayload{AccountID: "acc-1", Endpoint: testEndpoint, InstrumentID: "uid-1", OrderID: orderID, Lots: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339)},
	}
	params := orders.PlaceParams{
		AccountID: "acc-1", InstrumentID: "uid-1", OrderID: orderID,
		Direction: investapi.OrderDirection_ORDER_DIRECTION_BUY,
		OrderType: investapi.OrderType_ORDER_TYPE_LIMIT,
		Lots:      1, Price: &investapi.Quotation{Units: 100},
	}
	return intent, params
}

func TestPlaceHappyPath(t *testing.T) {
	fake := &fakeOrders{postResp: &investapi.PostOrderResponse{
		OrderId:               "exch-1",
		ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW,
		LotsRequested:         1,
	}}
	conn := newOrdersConn(t, fake)
	led := testLedger(t)

	intent, params := placeIntent("order-123")
	out, cerr := placeExec(context.Background(), orders.New(conn), led, intent, params, false)
	if cerr != nil {
		t.Fatalf("place failed: %+v", cerr)
	}
	if out.Sync.GetOrderId() != "exch-1" {
		t.Errorf("exchange order id = %q", out.Sync.GetOrderId())
	}
	if lc := orders.Lifecycle(out.Sync.GetExecutionReportStatus()); lc != orders.LifecycleNew {
		t.Errorf("lifecycle = %q, want new", lc)
	}
	// Ledger must have no unresolved entries: the intent ended Confirmed.
	unresolved, err := led.Unresolved()
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) != 0 {
		t.Errorf("want 0 unresolved after confirmed place, got %d", len(unresolved))
	}
	// The broker saw the exact client order_id.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.postOrderIDs) != 1 || fake.postOrderIDs[0] != "order-123" {
		t.Errorf("PostOrder order ids = %v, want [order-123]", fake.postOrderIDs)
	}
}

func TestPlaceBrokerRejection(t *testing.T) {
	fake := &fakeOrders{postResp: &investapi.PostOrderResponse{
		ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_REJECTED,
		Message:               "insufficient funds",
	}}
	conn := newOrdersConn(t, fake)
	led := testLedger(t)

	intent, params := placeIntent("order-rej")
	_, cerr := placeExec(context.Background(), orders.New(conn), led, intent, params, false)
	if cerr == nil {
		t.Fatal("want rejection error")
	}
	if cerr.ExitCode() != render.ExitRejected {
		t.Errorf("exit code = %d, want %d", cerr.ExitCode(), render.ExitRejected)
	}
	// Rejected is a definitive outcome → no unresolved entry.
	unresolved, _ := led.Unresolved()
	if len(unresolved) != 0 {
		t.Errorf("want 0 unresolved after rejection, got %d", len(unresolved))
	}
}

func TestPlaceAmbiguousExitSevenThenReconcile(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	received := make(chan struct{})
	fake := &fakeOrders{block: block, received: received}
	conn := newOrdersConn(t, fake)
	led := testLedger(t)

	// Cancel only after the server has demonstrably received the request, so
	// the call is guaranteed past the send phase (sent_unconfirmed, never
	// not_sent) regardless of scheduler timing.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-received
		cancel()
	}()

	intent, params := placeIntent("order-ambiguous")
	_, cerr := placeExec(ctx, orders.New(conn), led, intent, params, false)
	if cerr == nil {
		t.Fatal("want unconfirmed error")
	}
	if cerr.ExitCode() != render.ExitUnconfirmed {
		t.Fatalf("exit code = %d, want %d (exit 7)", cerr.ExitCode(), render.ExitUnconfirmed)
	}
	if cerr.ReconcileHint == nil || cerr.ReconcileHint.OrderID != "order-ambiguous" {
		t.Fatalf("reconcile hint = %+v, want order-ambiguous", cerr.ReconcileHint)
	}
	if want := "tinvest --profile test --account acc-1 orders reconcile"; cerr.ReconcileHint.Command != want {
		t.Fatalf("reconcile command = %q, want %q", cerr.ReconcileHint.Command, want)
	}

	// The ledger must hold the unresolved intent with its order_id.
	unresolved, err := led.Unresolved()
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) != 1 || unresolved[0].OrderID() != "order-ambiguous" {
		t.Fatalf("unresolved = %+v, want 1 entry order-ambiguous", unresolved)
	}

	// Now reconcile against a broker that DOES know the order.
	recFake := &fakeOrders{stateByRequest: map[string]*investapi.OrderState{
		"order-ambiguous": {
			OrderId:               "exch-9",
			ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL,
			LotsRequested:         1, LotsExecuted: 1,
		},
	}}
	recConn := newOrdersConn(t, recFake)
	outcomes, rcerr := reconcileFlowForTarget(
		context.Background(), orders.New(recConn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
		reconcileOptions{SyncNotFoundDelay: 0},
	)
	if rcerr != nil {
		t.Fatalf("reconcile failed: %+v", rcerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "placed" || outcomes[0].OrderID != "exch-9" {
		t.Fatalf("reconcile outcomes = %+v", outcomes)
	}
	if outcomes[0].Lifecycle != orders.LifecycleFilled {
		t.Errorf("lifecycle = %q, want filled", outcomes[0].Lifecycle)
	}
	// After reconcile the entry is resolved.
	after, _ := led.Unresolved()
	if len(after) != 0 {
		t.Errorf("want 0 unresolved after reconcile, got %d", len(after))
	}
}

func TestIntentCreatedOrdersReconcileNotPlacedWithoutBrokerCalls(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "async"}[async], func(t *testing.T) {
			led := testLedger(t)
			intent, _ := placeIntent("created-only")
			payload := intent.Payload.(orderPayload)
			payload.Async = async
			intent.Payload = payload
			if _, err := led.Begin(intent); err != nil {
				t.Fatal(err)
			}

			fake := &fakeOrders{stateErr: status.Error(codes.NotFound, "not sent")}
			outcomes, cerr := reconcileFlowForTarget(
				context.Background(), orders.New(newOrdersConn(t, fake)), led,
				reconcileTarget{Profile: "test", Endpoint: testEndpoint},
				reconcileOptions{SyncNotFoundDelay: 0},
			)
			if cerr != nil {
				t.Fatalf("reconcile: %+v", cerr)
			}
			if len(outcomes) != 1 || outcomes[0].Outcome != "not-placed" {
				t.Fatalf("outcomes = %+v, want not-placed", outcomes)
			}
			fake.mu.Lock()
			stateCalls, listCalls := fake.stateCalls, len(fake.getOrdersReqs)
			fake.mu.Unlock()
			if stateCalls != 0 || listCalls != 0 {
				t.Fatalf("broker calls: GetOrderState=%d GetOrders=%d, want zero", stateCalls, listCalls)
			}
		})
	}
}

func TestReconcileNotFoundMarksNotPlaced(t *testing.T) {
	led := testLedger(t)
	// Seed an unresolved intent directly.
	intent, _ := placeIntent("order-lost")
	entry, err := led.Begin(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}

	fake := &fakeOrders{stateErr: status.Error(codes.NotFound, "70001")}
	conn := newOrdersConn(t, fake)
	outcomes, cerr := reconcileFlowForTarget(
		context.Background(), orders.New(conn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
		reconcileOptions{SyncNotFoundDelay: 0},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "not-placed" {
		t.Fatalf("outcomes = %+v, want not-placed", outcomes)
	}
	if after, _ := led.Unresolved(); len(after) != 0 {
		t.Errorf("not-placed entry must be reconciled, got %d unresolved", len(after))
	}
}

func TestPlaceRetriesSameOrderIDOnUnavailable(t *testing.T) {
	fake := &fakeOrders{
		unavailableLeft: 1, // fail once, then succeed
		postResp: &investapi.PostOrderResponse{
			OrderId:               "exch-r",
			ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW,
			LotsRequested:         1,
		},
	}
	conn := newOrdersConn(t, fake)
	led := testLedger(t)

	intent, params := placeIntent("order-retry")
	out, cerr := placeExec(context.Background(), orders.New(conn), led, intent, params, false)
	if cerr != nil {
		t.Fatalf("place failed: %+v", cerr)
	}
	if out.Sync.GetOrderId() != "exch-r" {
		t.Errorf("order id = %q", out.Sync.GetOrderId())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.postOrderIDs) != 2 {
		t.Fatalf("want 2 PostOrder attempts (1 retry), got %d", len(fake.postOrderIDs))
	}
	if fake.postOrderIDs[0] != "order-retry" || fake.postOrderIDs[1] != "order-retry" {
		t.Errorf("retry used a different order_id: %v", fake.postOrderIDs)
	}
}

func TestWaitReachesTerminal(t *testing.T) {
	fake := &fakeOrders{stateResp: &investapi.OrderState{
		OrderId:               "exch-w",
		ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL,
		LotsRequested:         1, LotsExecuted: 1,
	}}
	conn := newOrdersConn(t, fake)

	state, _, cerr := waitFlow(context.Background(), orders.New(conn), "acc-1", "exch-w", false, 10*time.Millisecond, 2*time.Second)
	if cerr != nil {
		t.Fatalf("wait: %+v", cerr)
	}
	if !orders.IsTerminal(state.GetExecutionReportStatus()) {
		t.Errorf("want terminal state, got %v", state.GetExecutionReportStatus())
	}
}

func TestWaitTimesOut(t *testing.T) {
	fake := &fakeOrders{stateResp: &investapi.OrderState{
		ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW,
	}}
	conn := newOrdersConn(t, fake)

	_, _, cerr := waitFlow(context.Background(), orders.New(conn), "acc-1", "exch-x", false, 10*time.Millisecond, 60*time.Millisecond)
	if cerr == nil {
		t.Fatal("want timeout error")
	}
	if cerr.ExitCode() != render.ExitNetwork {
		t.Errorf("exit code = %d, want %d", cerr.ExitCode(), render.ExitNetwork)
	}
}

func TestDryRunTouchesNoLedger(t *testing.T) {
	fake := &fakeOrders{
		previewResp: &investapi.GetOrderPriceResponse{
			LotsRequested:    1,
			TotalOrderAmount: &investapi.MoneyValue{Units: 100, Currency: "rub"},
		},
		maxLotsResp: &investapi.GetMaxLotsResponse{
			Currency:  "rub",
			BuyLimits: &investapi.GetMaxLotsResponse_BuyLimitsView{BuyMaxLots: 42},
		},
	}
	conn := newOrdersConn(t, fake)

	dir := t.TempDir()
	a := &app{ledgerDir: dir}
	settings := config.Settings{AccountID: "acc-1", Profile: "test"}
	rp := resolvedPlace{
		instrument: "uid-1",
		direction:  investapi.OrderDirection_ORDER_DIRECTION_BUY,
		orderType:  investapi.OrderType_ORDER_TYPE_LIMIT,
		lots:       1, price: &investapi.Quotation{Units: 100}, dryRun: true,
	}

	stdout := captureStdout(t, func() {
		if err := a.runDryRun(context.Background(), orders.New(conn), settings, "uid-1", rp, time.Now(), "json"); err != nil {
			t.Fatalf("runDryRun: %v", err)
		}
	})

	if !strings.Contains(stdout, `"dry_run": true`) {
		t.Errorf("dry-run output missing dry_run flag:\n%s", stdout)
	}
	// The ledger directory must contain no journal file: dry-run never opened it.
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("dry-run created ledger files: %v", entries)
	}
}

// ---- JSON input round-trip ----

func TestPlaceInputRoundTrip(t *testing.T) {
	tmp := t.TempDir() + "/in.json"
	if err := os.WriteFile(tmp, []byte(`{
		"instrument": "e6123145-9665-43e0-8413-cd61b8aa9b13",
		"direction": "buy",
		"quantity": 3,
		"type": "limit",
		"price": "250.5",
		"tif": "day",
		"order_id": "00000000-0000-4000-8000-000000000003"
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	rp, cerr := resolvePlaceInput(tmp)
	if cerr != nil {
		t.Fatalf("resolvePlaceInput: %+v", cerr)
	}
	if rp.lots != 3 || rp.orderID != "00000000-0000-4000-8000-000000000003" {
		t.Errorf("parsed = %+v", rp)
	}
	if rp.direction != investapi.OrderDirection_ORDER_DIRECTION_BUY {
		t.Errorf("direction = %v", rp.direction)
	}
	if rp.price.GetUnits() != 250 || rp.price.GetNano() != 500000000 {
		t.Errorf("price = %+v", rp.price)
	}
}

func TestPlaceInputRejectsUnknownField(t *testing.T) {
	tmp := t.TempDir() + "/in.json"
	if err := os.WriteFile(tmp, []byte(`{"instrument":"uid","direction":"buy","quantity":1,"type":"market","bogus":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, cerr := resolvePlaceInput(tmp)
	if cerr == nil {
		t.Fatal("want error for unknown field")
	}
	if cerr.ExitCode() != render.ExitUsage {
		t.Errorf("exit code = %d, want %d", cerr.ExitCode(), render.ExitUsage)
	}
}

// ---- golden envelope for the exit-7 unknown-state error ----

func TestExitSevenEnvelopeGolden(t *testing.T) {
	cerr := render.UnconfirmedError(
		"order outcome unconfirmed; reconcile required",
		"order-ambiguous",
		reconcileCommand,
		render.CallContext{Phase: transport.PhaseSentUnconfirmed, Mutation: true},
	)
	env := render.Failure(cerr, render.NewMeta("acc-1", "", 0))
	got, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	golden := `{
  "ok": false,
  "error": {
    "code": "UNCONFIRMED",
    "message": "order outcome unconfirmed; reconcile required",
    "retryable": false,
    "phase": "sent_unconfirmed",
    "reconcile": {
      "order_id": "order-ambiguous",
      "command": "tinvest orders reconcile"
    }
  },
  "meta": {
    "account_id": "acc-1",
    "elapsed_ms": 0,
    "contract": "1.49",
    "schema_version": "0.1"
  }
}`
	if strings.TrimSpace(string(got)) != strings.TrimSpace(golden) {
		t.Errorf("exit-7 envelope mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, golden)
	}
}

// ---- helpers ----

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				sb.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
		done <- sb.String()
	}()
	fn()
	_ = w.Close()
	os.Stdout = orig
	return <-done
}
