package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"tinvest/internal/broker/orders"
	"tinvest/internal/broker/stoporders"
	"tinvest/internal/config"
	"tinvest/internal/ledger"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
)

// captureStderr redirects os.Stderr for the duration of fn and returns what was
// written, mirroring captureStdout.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
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
	os.Stderr = orig
	return <-done
}

// ---- F5: cancel NOT_FOUND verifies actual state ----

func TestCancelSettledNote(t *testing.T) {
	tests := []struct {
		name   string
		fake   *fakeOrders
		wantOK bool
	}{
		{
			name:   "already terminal is a satisfied no-op",
			fake:   &fakeOrders{stateResp: &investapi.OrderState{OrderId: "x", ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED}},
			wantOK: true,
		},
		{
			name:   "still active keeps the rejection path",
			fake:   &fakeOrders{stateResp: &investapi.OrderState{OrderId: "x", ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW}},
			wantOK: false,
		},
		{
			name:   "never-valid id (NOT_FOUND) keeps the rejection path",
			fake:   &fakeOrders{}, // GetOrderState returns NOT_FOUND
			wantOK: false,
		},
		{
			name: "terminal order visible only in the day list is a satisfied no-op",
			fake: &fakeOrders{
				stateErr: status.Error(codes.NotFound, "terminal state omitted"),
				todayOnlyOrders: []*investapi.OrderState{{
					OrderId: "exch-1", ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED,
				}},
			},
			wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newOrdersConn(t, tt.fake)
			note, ok := cancelSettledNote(context.Background(), orders.New(conn), "acc-1", "exch-1", false)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (note=%q)", ok, tt.wantOK, note)
			}
			if ok && !strings.Contains(note, "terminal") {
				t.Errorf("note = %q, want it to explain the terminal no-op", note)
			}
		})
	}
}

func TestCancelRetryNotFoundUsesTerminalDayListAndExitsZero(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv(config.EnvToken, "test-token")
	fake := &fakeOrders{
		cancelReplies: []cancelReply{
			{err: status.Error(codes.Unavailable, "cancel applied but response lost")},
			{err: status.Error(codes.NotFound, "already removed from active orders")},
		},
		todayOnlyOrders: []*investapi.OrderState{{
			OrderId: "exchange-1", ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED,
		}},
	}
	conn := newOrdersConn(t, fake)
	a := &app{connectOverride: func(context.Context, config.Settings) (*grpc.ClientConn, *render.CLIError) {
		return conn, nil
	}}
	root := a.rootCmd()
	root.SetArgs([]string{"--account", "acc-1", "--output", "json", "orders", "cancel", "exchange-1"})

	var executionErr error
	output := captureStdout(t, func() { executionErr = root.Execute() })
	if executionErr != nil {
		t.Fatalf("cancel command error = %v, output = %s", executionErr, output)
	}
	fake.mu.Lock()
	cancelCalls := fake.cancelCalls
	fake.mu.Unlock()
	if cancelCalls != 2 {
		t.Fatalf("CancelOrder calls = %d, want 2 (Unavailable retry then NotFound)", cancelCalls)
	}
	if !strings.Contains(output, `"ok": true`) || !strings.Contains(output, `"note": "order was already terminal`) {
		t.Fatalf("output = %s, want successful settled-note envelope", output)
	}
}

func TestStopCancelSettledNote(t *testing.T) {
	terminal := &investapi.StopOrder{StopOrderId: "s-1", Status: investapi.StopOrderStatusOption_STOP_ORDER_STATUS_CANCELED}
	active := &investapi.StopOrder{StopOrderId: "s-1", Status: investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ACTIVE}
	tests := []struct {
		name   string
		list   []*investapi.StopOrder
		wantOK bool
	}{
		{name: "terminal in status=ALL is a satisfied no-op", list: []*investapi.StopOrder{terminal}, wantOK: true},
		{name: "still active keeps the rejection path", list: []*investapi.StopOrder{active}, wantOK: false},
		{name: "absent id keeps the rejection path", list: nil, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeStopOrders{listResp: tt.list}
			conn := newStopOrdersConn(t, fake)
			note, ok := stopCancelSettledNote(context.Background(), stoporders.New(conn), "acc-1", "s-1")
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (note=%q)", ok, tt.wantOK, note)
			}
			if ok {
				if !strings.Contains(note, "terminal") {
					t.Errorf("note = %q, want it to explain the terminal no-op", note)
				}
				// The lookup must have used status=ALL.
				if len(fake.listRequests) != 1 || fake.listRequests[0].GetStatus() != investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ALL {
					t.Errorf("list requests = %+v, want one status=ALL", fake.listRequests)
				}
			}
		})
	}
}

// ---- F6: replace rejection message ----

func TestReplaceRejectMessage(t *testing.T) {
	if got := replaceRejectMessage(&investapi.PostOrderResponse{Message: "insufficient funds"}); got != "insufficient funds" {
		t.Errorf("message = %q, want the broker message", got)
	}
	if got := replaceRejectMessage(&investapi.PostOrderResponse{}); !strings.Contains(got, "rejected") {
		t.Errorf("fallback = %q, want a generic rejection phrase", got)
	}
	// The rejection decision reuses orders.IsRejected, exactly as placeExec does.
	if !orders.IsRejected(investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_REJECTED) {
		t.Fatal("a REJECTED replace response must classify as rejected")
	}
}

// ---- F7: post-mutation output failure degrades to exit 1, not usage ----

func TestEmitMutationResult(t *testing.T) {
	// A failing write after a successful mutation must exit 1 (internal), not the
	// cobra usage exit 2, and must name the order id on stderr.
	var err error
	errOut := captureStderr(t, func() {
		err = emitMutationResult(func() error { return errors.New("broken pipe") }, "order-xyz", "was placed")
	})
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != render.ExitInternal {
		t.Fatalf("err = %v, want exitError{1}", err)
	}
	if !strings.Contains(errOut, "order-xyz") || !strings.Contains(errOut, "was placed") {
		t.Errorf("stderr = %q, want it to preserve the order id and outcome", errOut)
	}
	// A successful write returns nil.
	if err := emitMutationResult(func() error { return nil }, "order-xyz", "was placed"); err != nil {
		t.Errorf("successful write returned %v, want nil", err)
	}
}

// ---- F16: post-outcome journal write failures warn on stderr ----

func TestWarnJournalWrite(t *testing.T) {
	out := captureStderr(t, func() {
		warnJournalWrite("broker-confirmed", reconcileCommand, errors.New("disk full"))
	})
	if !strings.Contains(out, "disk full") || !strings.Contains(out, reconcileCommand) {
		t.Errorf("stderr = %q, want a warning naming the error and the reconcile command", out)
	}
	if out := captureStderr(t, func() { warnJournalWrite("broker-confirmed", reconcileCommand, nil) }); out != "" {
		t.Errorf("no error should produce no warning, got %q", out)
	}
}

// ---- F19: reconcile counts unresolved outcomes and exits non-zero ----

func TestReconcileDataCountsAndFinishExit(t *testing.T) {
	if reconcileNeedsAttention("placed") || reconcileNeedsAttention("not-placed") || reconcileNeedsAttention("foreign") || reconcileNeedsAttention("profile-mismatch") {
		t.Fatal("clean and deliberate-skip outcomes must not need attention")
	}
	for _, o := range []string{"indeterminate", "error", "unresolved", "ambiguous"} {
		if !reconcileNeedsAttention(o) {
			t.Fatalf("%q must need attention", o)
		}
	}

	data := newReconcileData([]render.ReconcileOutcomeView{
		{Outcome: "placed"},
		{Outcome: "unresolved"},
		{Outcome: "indeterminate"},
	}, stopReconcileCommand)
	if data.UnresolvedCount != 2 {
		t.Fatalf("unresolved count = %d, want 2", data.UnresolvedCount)
	}

	var err error
	_ = captureStdout(t, func() { err = finishReconcile("json", data, render.NewMeta("acc-1", "", 0)) })
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != render.ExitInternal {
		t.Fatalf("finishReconcile err = %v, want exitError{1} for a partial sweep", err)
	}

	clean := newReconcileData([]render.ReconcileOutcomeView{{Outcome: "placed"}, {Outcome: "not-placed"}}, stopReconcileCommand)
	var cleanErr error
	_ = captureStdout(t, func() { cleanErr = finishReconcile("json", clean, render.NewMeta("acc-1", "", 0)) })
	if cleanErr != nil {
		t.Fatalf("a clean sweep must exit 0, got %v", cleanErr)
	}
}

// ---- F14: sandbox exit-7 envelope carries recovery guidance ----

func TestSandboxUnknownStateGuidance(t *testing.T) {
	unconfirmed := &render.CLIError{Code: render.CodeUnconfirmed, Message: "sandbox open outcome unknown"}
	got := sandboxUnknownStateGuidance(unconfirmed)
	if !strings.Contains(got.Message, sandboxInspectCommand) {
		t.Errorf("message = %q, want it to name %q", got.Message, sandboxInspectCommand)
	}
	if got.ReconcileHint == nil || got.ReconcileHint.Command != sandboxInspectCommand {
		t.Errorf("reconcile hint = %+v, want the inspect command", got.ReconcileHint)
	}
	if got.ReconcileHint != nil && got.ReconcileHint.OrderID != "" {
		t.Errorf("sandbox has no order id, want empty OrderID, got %q", got.ReconcileHint.OrderID)
	}

	// A non-exit-7 error is left untouched (no dangling reconcile hint).
	rejected := &render.CLIError{Code: render.CodeBrokerRejected, Message: "bad request"}
	if got := sandboxUnknownStateGuidance(rejected); got.ReconcileHint != nil || got.Message != "bad request" {
		t.Errorf("non-unconfirmed error was modified: %+v", got)
	}
}

// ---- F17: cobra usage errors honor an explicit -o flag from args ----

func TestOutputMode(t *testing.T) {
	t.Setenv("TINVEST_OUTPUT", "")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"separate -o", []string{"bogus", "-o", "json"}, "json"},
		{"separate --output", []string{"--output", "table", "bogus"}, "table"},
		{"joined --output=", []string{"bogus", "--output=json"}, "json"},
		{"joined -o=", []string{"bogus", "-o=table"}, "table"},
		{"joined -ojson", []string{"bogus", "-ojson"}, "json"},
		{"none falls back to env", []string{"bogus"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := outputMode(tt.args); got != tt.want {
				t.Errorf("outputMode(%v) = %q, want %q", tt.args, got, tt.want)
			}
		})
	}
	// With no flag, the environment wins.
	t.Setenv("TINVEST_OUTPUT", "json")
	if got := outputMode([]string{"bogus"}); got != "json" {
		t.Errorf("outputMode with env set = %q, want json", got)
	}
}

// ---- F10: stop reconcile requires a unique, in-window match and notes heuristics ----

func stopMatchListEntry(id string, created time.Time) *investapi.StopOrder {
	return &investapi.StopOrder{
		StopOrderId:   id,
		InstrumentUid: "uid-1",
		Direction:     investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY,
		OrderType:     investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
		LotsRequested: 1,
		StopPrice:     &investapi.MoneyValue{Units: 100},
		CreateDate:    timestamppb.New(created),
		Status:        investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ACTIVE,
	}
}

func reconcileOneStop(t *testing.T, intentID string, list []*investapi.StopOrder) (render.ReconcileOutcomeView, *ledger.Ledger) {
	t.Helper()
	led := testLedger(t)
	intent, _ := stopPlaceIntent(intentID)
	entry, err := led.Begin(intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := entry.SendStarted(); err != nil {
		t.Fatal(err)
	}
	fake := &fakeStopOrders{listResp: list}
	conn := newStopOrdersConn(t, fake)
	outcomes, cerr := reconcileStopFlowForTarget(
		context.Background(), stoporders.New(conn), led,
		reconcileTarget{Profile: "test", Endpoint: testEndpoint},
	)
	if cerr != nil {
		t.Fatalf("reconcile: %+v", cerr)
	}
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %+v, want 1", outcomes)
	}
	return outcomes[0], led
}

func TestStopReconcilePlacedIsUniqueInWindowWithHeuristicNote(t *testing.T) {
	out, led := reconcileOneStop(t, "stop-unique", []*investapi.StopOrder{stopMatchListEntry("s-uniq", time.Now())})
	if out.Outcome != "placed" || out.OrderID != "s-uniq" {
		t.Fatalf("outcome = %+v, want placed s-uniq", out)
	}
	if !strings.Contains(out.Note, "heuristic") {
		t.Errorf("note = %q, want it to flag heuristic correlation", out.Note)
	}
	if after, _ := led.Unresolved(); len(after) != 0 {
		t.Errorf("a unique in-window match must resolve the intent, got %d unresolved", len(after))
	}
}

func TestStopReconcileUniqueOutOfWindowStaysUnresolved(t *testing.T) {
	old := time.Now().Add(-30 * time.Minute)
	out, led := reconcileOneStop(t, "stop-old", []*investapi.StopOrder{stopMatchListEntry("s-old", old)})
	if out.Outcome != "unresolved" {
		t.Fatalf("outcome = %+v, want unresolved (unique but out of window)", out)
	}
	if after, _ := led.Unresolved(); len(after) != 1 {
		t.Errorf("out-of-window match must stay unresolved, got %d unresolved", len(after))
	}
}

func TestStopReconcileNonUniqueStaysAmbiguous(t *testing.T) {
	now := time.Now()
	out, led := reconcileOneStop(t, "stop-two", []*investapi.StopOrder{
		stopMatchListEntry("s-a", now),
		stopMatchListEntry("s-b", now),
	})
	if out.Outcome != "ambiguous" {
		t.Fatalf("outcome = %+v, want ambiguous (two field matches)", out)
	}
	if after, _ := led.Unresolved(); len(after) != 1 {
		t.Errorf("ambiguous match must stay unresolved, got %d unresolved", len(after))
	}
}
