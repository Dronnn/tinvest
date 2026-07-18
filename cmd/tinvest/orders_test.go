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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"tinvest/internal/broker/orders"
	"tinvest/internal/config"
	"tinvest/internal/ledger"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	"tinvest/internal/transport"
	"tinvest/internal/transport/retry"
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

	stateByRequest map[string]*investapi.OrderState // keyed by client order_id
	stateResp      *investapi.OrderState            // fallback for any lookup
	stateErr       error
	stateCalls     int

	openOrders []*investapi.OrderState

	previewResp *investapi.GetOrderPriceResponse
	maxLotsResp *investapi.GetMaxLotsResponse
}

func (f *fakeOrders) PostOrder(ctx context.Context, req *investapi.PostOrderRequest) (*investapi.PostOrderResponse, error) {
	f.mu.Lock()
	f.postOrderIDs = append(f.postOrderIDs, req.GetOrderId())
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

func (f *fakeOrders) GetOrders(_ context.Context, _ *investapi.GetOrdersRequest) (*investapi.GetOrdersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	srv := grpc.NewServer()
	investapi.RegisterOrdersServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	policy := retry.DefaultRetryPolicy()
	conn, err := transport.Dial(context.Background(), transport.Config{
		Endpoint:    "passthrough:///bufnet",
		Token:       "test-token",
		Credentials: insecure.NewCredentials(),
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
		Payload: orderPayload{AccountID: "acc-1", InstrumentID: "uid-1", OrderID: orderID, Lots: 1},
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
	fake := &fakeOrders{block: block}
	conn := newOrdersConn(t, fake)
	led := testLedger(t)

	// Short deadline so the blocked PostOrder times out as sent_unconfirmed.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

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
	outcomes, rcerr := reconcileFlow(context.Background(), orders.New(recConn), led)
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
	outcomes, cerr := reconcileFlow(context.Background(), orders.New(conn), led)
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
		"order_id": "my-key-1"
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	rp, cerr := resolvePlaceInput(tmp)
	if cerr != nil {
		t.Fatalf("resolvePlaceInput: %+v", cerr)
	}
	if rp.lots != 3 || rp.orderID != "my-key-1" {
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
