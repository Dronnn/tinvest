package main

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"tinvest/internal/broker/stoporders"
	"tinvest/internal/ledger"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	"tinvest/internal/transport"
	"tinvest/internal/transport/retry"
)

// fakeStopOrders is an in-process StopOrdersService scripting the behaviors
// the stop-place flow tests need: block-forever (exit-7), transient
// UNAVAILABLE (must NOT be retried — plan §9), and a stop-order list for
// reconcile's list-match.
type fakeStopOrders struct {
	investapi.UnimplementedStopOrdersServiceServer

	mu sync.Mutex

	postOrderIDs    []string // client order_id seen on each PostStopOrder attempt
	unavailableLeft int
	postResp        *investapi.PostStopOrderResponse
	postErr         error
	block           chan struct{}

	listResp     []*investapi.StopOrder
	listErr      error
	listRequests []*investapi.GetStopOrdersRequest
}

func (f *fakeStopOrders) PostStopOrder(ctx context.Context, req *investapi.PostStopOrderRequest) (*investapi.PostStopOrderResponse, error) {
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
		return nil, status.Error(codes.DeadlineExceeded, "blocked")
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (f *fakeStopOrders) GetStopOrders(_ context.Context, req *investapi.GetStopOrdersRequest) (*investapi.GetStopOrdersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listRequests = append(f.listRequests, req)
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &investapi.GetStopOrdersResponse{StopOrders: f.listResp}, nil
}

// newStopOrdersConn dials the fake over bufconn with the default retry policy
// installed (so we can prove ineligible mutations are NOT retried), mirroring
// newOrdersConn in orders_test.go.
func newStopOrdersConn(t *testing.T, f *fakeStopOrders) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	investapi.RegisterStopOrdersServiceServer(srv, f)
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

func stopPlaceIntent(orderID string) (ledger.Intent, stoporders.PlaceParams) {
	createdAt := time.Now().UTC()
	intent := ledger.Intent{
		IntentID: orderID, Kind: kindStopOrderPlace, AccountID: "acc-1",
		Profile: "test", Attempt: 1, OrderID: orderID,
		Payload: stopOrderPayload{
			AccountID: "acc-1", Endpoint: testEndpoint, InstrumentID: "uid-1", OrderID: orderID,
			Direction:      investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY.String(),
			StopOrderType:  investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS.String(),
			Quantity:       1,
			StopPrice:      "100",
			ExpirationType: investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL.String(),
			CreatedAt:      createdAt.Format(time.RFC3339Nano),
		},
	}
	params := stoporders.PlaceParams{
		AccountID: "acc-1", InstrumentID: "uid-1", OrderID: orderID,
		Direction:      investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
		StopOrderType:  investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
		Quantity:       1,
		StopPrice:      &investapi.Quotation{Units: 100},
		ExpirationType: investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
	}
	return intent, params
}

func TestStopPlaceHappyPath(t *testing.T) {
	fake := &fakeStopOrders{postResp: &investapi.PostStopOrderResponse{StopOrderId: "stop-exch-1"}}
	conn := newStopOrdersConn(t, fake)
	led := testLedger(t)

	intent, params := stopPlaceIntent("stop-order-123")
	resp, info, cerr := placeStopExec(context.Background(), stoporders.New(conn), led, intent, params)
	if cerr != nil {
		t.Fatalf("place failed: %+v", cerr)
	}
	if info == nil {
		t.Fatal("want non-nil call info")
	}
	if resp.GetStopOrderId() != "stop-exch-1" {
		t.Errorf("stop order id = %q", resp.GetStopOrderId())
	}

	// The envelope carries stop_order_id and the client order_id.
	view := render.PlaceStopResult(resp, "stop-order-123")
	if view.StopOrderID != "stop-exch-1" || view.ClientOrderID != "stop-order-123" {
		t.Errorf("place-stop view = %+v", view)
	}

	// Ledger must have no unresolved entries: the intent ended Confirmed.
	unresolved, err := led.Unresolved()
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) != 0 {
		t.Errorf("want 0 unresolved after confirmed place, got %d", len(unresolved))
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.postOrderIDs) != 1 || fake.postOrderIDs[0] != "stop-order-123" {
		t.Errorf("PostStopOrder order ids = %v, want [stop-order-123]", fake.postOrderIDs)
	}
}

// TestStopPlaceNoRetryOnUnavailable is the key safety test for M1e (plan §9):
// unlike orders place, a stop-order placement context is never marked
// retry.Idempotent, so a transient UNAVAILABLE must surface as an error on
// the first attempt rather than being retried — proven here by asserting the
// fake server saw exactly one PostStopOrder call even though the retry
// interceptor is installed and would happily retry an eligible call.
func TestStopPlaceNoRetryOnUnavailable(t *testing.T) {
	fake := &fakeStopOrders{unavailableLeft: 5} // would keep failing every attempt if retried
	conn := newStopOrdersConn(t, fake)
	led := testLedger(t)

	intent, params := stopPlaceIntent("stop-no-retry")
	_, _, cerr := placeStopExec(context.Background(), stoporders.New(conn), led, intent, params)
	if cerr == nil {
		t.Fatal("want an error: UNAVAILABLE must surface, not be retried away")
	}

	fake.mu.Lock()
	attempts := len(fake.postOrderIDs)
	fake.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("PostStopOrder attempts = %d, want exactly 1 (no auto-retry for stop-order placement, plan §9)", attempts)
	}
}

// TestStopPlaceAmbiguousExitSevenThenReconcile covers the exit-7 unknown-state
// protocol and the list-match reconcile path (plan §9): a send that never
// gets a definitive answer leaves the ledger entry unresolved and carries a
// reconcile hint; `stop-orders reconcile` then finds the stop order by
// listing and matching on the journaled request shape.
func TestStopPlaceAmbiguousExitSevenThenReconcile(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	fake := &fakeStopOrders{block: block}
	conn := newStopOrdersConn(t, fake)
	led := testLedger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	intent, params := stopPlaceIntent("stop-ambiguous")
	_, _, cerr := placeStopExec(ctx, stoporders.New(conn), led, intent, params)
	if cerr == nil {
		t.Fatal("want unconfirmed error")
	}
	if cerr.ExitCode() != render.ExitUnconfirmed {
		t.Fatalf("exit code = %d, want %d (exit 7)", cerr.ExitCode(), render.ExitUnconfirmed)
	}
	if cerr.ReconcileHint == nil || cerr.ReconcileHint.OrderID != "stop-ambiguous" {
		t.Fatalf("reconcile hint = %+v, want stop-ambiguous", cerr.ReconcileHint)
	}
	if cerr.ReconcileHint.Command != stopReconcileCommand {
		t.Errorf("reconcile command = %q, want %q", cerr.ReconcileHint.Command, stopReconcileCommand)
	}

	unresolved, err := led.Unresolved()
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) != 1 || unresolved[0].OrderID() != "stop-ambiguous" {
		t.Fatalf("unresolved = %+v, want 1 entry stop-ambiguous", unresolved)
	}

	// Reconcile against a broker whose stop-order list includes exactly one
	// order matching the journaled shape (instrument, direction, quantity,
	// stop price) — GetStopOrders carries no client order_id to look up
	// directly (see reconcileStopFlow's doc comment), so this is the only
	// available signal.
	recFake := &fakeStopOrders{listResp: []*investapi.StopOrder{
		{
			StopOrderId:   "stop-exch-9",
			InstrumentUid: "uid-1",
			Direction:     investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
			OrderType:     investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
			LotsRequested: 1,
			StopPrice:     &investapi.MoneyValue{Units: 100},
			CreateDate:    timestamppb.Now(),
			Status:        investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ACTIVE,
		},
	}}
	recConn := newStopOrdersConn(t, recFake)
	outcomes, rcerr := reconcileStopFlowForTarget(
		context.Background(), stoporders.New(recConn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
	)
	if rcerr != nil {
		t.Fatalf("reconcile failed: %+v", rcerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "placed" || outcomes[0].OrderID != "stop-exch-9" {
		t.Fatalf("reconcile outcomes = %+v", outcomes)
	}
	if outcomes[0].Lifecycle != stoporders.StatusActive {
		t.Errorf("lifecycle = %q, want %q", outcomes[0].Lifecycle, stoporders.StatusActive)
	}
	after, _ := led.Unresolved()
	if len(after) != 0 {
		t.Errorf("want 0 unresolved after reconcile, got %d", len(after))
	}
}

// TestReconcileStopAmbiguousLeavesUnresolved proves that when the list-match
// is not unique, reconcile reports it honestly as ambiguous instead of
// guessing, and leaves the entry unresolved for a human or a later run.
func TestReconcileStopAmbiguousLeavesUnresolved(t *testing.T) {
	led := testLedger(t)
	intent, _ := stopPlaceIntent("stop-dup")
	entry, err := led.Begin(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}

	dup := &investapi.StopOrder{
		InstrumentUid: "uid-1",
		Direction:     investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
		OrderType:     investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
		LotsRequested: 1,
		StopPrice:     &investapi.MoneyValue{Units: 100},
		CreateDate:    timestamppb.Now(),
	}
	fake := &fakeStopOrders{listResp: []*investapi.StopOrder{
		{StopOrderId: "a", InstrumentUid: dup.InstrumentUid, Direction: dup.Direction, OrderType: dup.OrderType, LotsRequested: dup.LotsRequested, StopPrice: dup.StopPrice, ExchangeOrderType: dup.ExchangeOrderType, CreateDate: dup.CreateDate},
		{StopOrderId: "b", InstrumentUid: dup.InstrumentUid, Direction: dup.Direction, OrderType: dup.OrderType, LotsRequested: dup.LotsRequested, StopPrice: dup.StopPrice, ExchangeOrderType: dup.ExchangeOrderType, CreateDate: dup.CreateDate},
	}}
	conn := newStopOrdersConn(t, fake)
	outcomes, cerr := reconcileStopFlowForTarget(
		context.Background(), stoporders.New(conn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "ambiguous" {
		t.Fatalf("outcomes = %+v, want ambiguous", outcomes)
	}
	after, _ := led.Unresolved()
	if len(after) != 1 {
		t.Errorf("ambiguous entry must remain unresolved, got %d unresolved", len(after))
	}
}

func TestReconcileStopZeroMatchesAfterSendStaysUnresolved(t *testing.T) {
	led := testLedger(t)
	intent, _ := stopPlaceIntent("stop-lost")
	entry, err := led.Begin(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}

	fake := &fakeStopOrders{} // empty list: nothing matches
	conn := newStopOrdersConn(t, fake)
	outcomes, cerr := reconcileStopFlowForTarget(
		context.Background(), stoporders.New(conn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 || outcomes[0].Outcome != "indeterminate" || !strings.Contains(outcomes[0].Error, "manual") {
		t.Fatalf("outcomes = %+v, want indeterminate with manual-check hint", outcomes)
	}
	if after, _ := led.Unresolved(); len(after) != 1 {
		t.Errorf("zero-match sent intent must remain unresolved, got %d unresolved", len(after))
	}
}

func TestStopReconcileSkipsForeignKindsAndMismatchedTargets(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		profile  string
		endpoint string
		outcome  string
		want     string
	}{
		{name: "foreign kind", kind: kindOrderPlace, profile: "test", endpoint: testEndpoint, outcome: "foreign", want: reconcileCommand},
		{name: "foreign profile", kind: kindStopOrderPlace, profile: "sandbox", endpoint: testEndpoint, outcome: "profile-mismatch", want: "--profile sandbox"},
		{name: "foreign endpoint", kind: kindStopOrderPlace, profile: "test", endpoint: "sandbox.example:443", outcome: "profile-mismatch", want: "sandbox.example:443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			led := testLedger(t)
			entry, err := led.Begin(ledger.Intent{
				IntentID: "stop-scope-" + tt.name, Kind: tt.kind, AccountID: "acc-1",
				Profile: tt.profile, OrderID: testUUID,
				Payload: map[string]any{"endpoint": tt.endpoint},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := entry.SendStarted(); err != nil {
				t.Fatal(err)
			}
			fake := &fakeStopOrders{}
			conn := newStopOrdersConn(t, fake)
			outcomes, cerr := reconcileStopFlowForTarget(
				context.Background(), stoporders.New(conn), led,
				reconcileTarget{Profile: "test", Endpoint: testEndpoint},
			)
			if cerr != nil {
				t.Fatalf("reconcile: %+v", cerr)
			}
			if len(outcomes) != 1 || outcomes[0].Outcome != tt.outcome || !strings.Contains(outcomes[0].Error, tt.want) {
				t.Fatalf("scope outcome = %+v", outcomes)
			}
			fake.mu.Lock()
			listCalls := len(fake.listRequests)
			fake.mu.Unlock()
			if listCalls != 0 {
				t.Fatalf("GetStopOrders calls = %d, want 0", listCalls)
			}
			if unresolved, _ := led.Unresolved(); len(unresolved) != 1 {
				t.Fatalf("skipped intent was resolved, unresolved = %d", len(unresolved))
			}
		})
	}
}

func TestStopReconcileUsesStatusAll(t *testing.T) {
	led := testLedger(t)
	intent, _ := stopPlaceIntent(testUUID)
	entry, err := led.Begin(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeStopOrders{}
	conn := newStopOrdersConn(t, fake)
	_, cerr := reconcileStopFlowForTarget(
		context.Background(), stoporders.New(conn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.listRequests) != 1 || fake.listRequests[0].GetStatus() != investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ALL {
		t.Fatalf("GetStopOrders requests = %+v, want one status=ALL", fake.listRequests)
	}
}

func TestMatchStopOrdersUsesFullRequestAndCreationWindow(t *testing.T) {
	created := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	payload := stopOrderPayload{
		InstrumentID:       "uid-1",
		Direction:          investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY.String(),
		StopOrderType:      investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT.String(),
		Quantity:           1,
		Price:              "99",
		StopPrice:          "100",
		ExpirationType:     investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE.String(),
		ExpireDate:         created.Add(time.Hour).Format(time.RFC3339),
		ExchangeOrderType:  investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_LIMIT.String(),
		TakeProfitType:     investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING.String(),
		TrailingIndent:     "1",
		TrailingIndentType: investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE.String(),
		TrailingSpread:     "0.5",
		TrailingSpreadType: investapi.TrailingValueType_TRAILING_VALUE_RELATIVE.String(),
		CreatedAt:          created.Format(time.RFC3339Nano),
	}
	exact := &investapi.StopOrder{
		StopOrderId: "exact", InstrumentUid: "uid-1",
		Direction:         investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
		OrderType:         investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT,
		LotsRequested:     1,
		Price:             &investapi.MoneyValue{Units: 99},
		StopPrice:         &investapi.MoneyValue{Units: 100},
		CreateDate:        timestamppb.New(created.Add(10 * time.Second)),
		ExpirationTime:    timestamppb.New(created.Add(time.Hour)),
		ExchangeOrderType: investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_LIMIT,
		TakeProfitType:    investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING,
		TrailingData: &investapi.StopOrder_TrailingData{
			Indent:     &investapi.Quotation{Units: 1},
			IndentType: investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE,
			Spread:     &investapi.Quotation{Nano: 500_000_000},
			SpreadType: investapi.TrailingValueType_TRAILING_VALUE_RELATIVE,
		},
	}
	wrongType := proto.Clone(exact).(*investapi.StopOrder)
	wrongType.StopOrderId = "wrong-type"
	wrongType.OrderType = investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS
	tooOld := proto.Clone(exact).(*investapi.StopOrder)
	tooOld.StopOrderId = "too-old"
	tooOld.CreateDate = timestamppb.New(created.Add(-10 * time.Minute))
	wrongTrailing := proto.Clone(exact).(*investapi.StopOrder)
	wrongTrailing.StopOrderId = "wrong-trailing"
	wrongTrailing.TrailingData = &investapi.StopOrder_TrailingData{
		Indent:     &investapi.Quotation{Units: 2},
		IndentType: investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE,
		Spread:     &investapi.Quotation{Nano: 500_000_000},
		SpreadType: investapi.TrailingValueType_TRAILING_VALUE_RELATIVE,
	}

	matches := matchStopOrders([]*investapi.StopOrder{wrongType, tooOld, wrongTrailing, exact}, payload)
	if len(matches) != 1 || matches[0].GetStopOrderId() != "exact" {
		t.Fatalf("matches = %+v, want only exact", matches)
	}
}

func TestTakeProfitPayloadUsesBrokerEnumStringsAndCanMatch(t *testing.T) {
	created := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	rp, cerr := buildStopPlace(stopPlaceInput{
		Instrument: testUUID, Direction: "buy", Quantity: 1, Type: "take-profit",
		StopPrice: "100", ExchangeOrderType: "limit", TakeProfitType: "trailing",
		TrailingIndent: "1", TrailingIndentType: "absolute",
		TrailingSpread: "0.5", TrailingSpreadType: "relative", OrderID: testUUID,
	})
	if cerr != nil {
		t.Fatalf("buildStopPlace: %+v", cerr)
	}
	payload := stopOrderPayloadFrom("acc-1", testEndpoint, "uid-1", created, rp)
	if payload.ExchangeOrderType != investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_LIMIT.String() ||
		payload.TakeProfitType != investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING.String() ||
		payload.TrailingIndentType != investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE.String() ||
		payload.TrailingSpreadType != investapi.TrailingValueType_TRAILING_VALUE_RELATIVE.String() {
		t.Fatalf("payload enum strings = %+v", payload)
	}

	stop := &investapi.StopOrder{
		StopOrderId: "exact", InstrumentUid: "uid-1", LotsRequested: 1,
		Direction:         investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
		OrderType:         investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT,
		StopPrice:         &investapi.MoneyValue{Units: 100},
		CreateDate:        timestamppb.New(created.Add(time.Second)),
		ExchangeOrderType: investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_LIMIT,
		TakeProfitType:    investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING,
		TrailingData: &investapi.StopOrder_TrailingData{
			Indent: &investapi.Quotation{Units: 1}, IndentType: investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE,
			Spread: &investapi.Quotation{Nano: 500_000_000}, SpreadType: investapi.TrailingValueType_TRAILING_VALUE_RELATIVE,
		},
	}
	matches := matchStopOrders([]*investapi.StopOrder{stop}, payload)
	if len(matches) != 1 || matches[0].GetStopOrderId() != "exact" {
		t.Fatalf("matches = %+v, want generated payload to match", matches)
	}
}
