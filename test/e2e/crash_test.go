package e2e

import (
	"bytes"
	"context"
	"testing"
	"time"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// TestKillAfterSendThenReconcile is the core crash-injection case (plan §9,
// task 2b). The fake signals the instant PostOrder arrives and then blocks
// forever, so the CLI is SIGKILLed while the call is in flight — after the
// write-ahead stages are fsynced but before any confirmation could be written.
//
// The timing is deterministic, not raced: placeExec journals and fsyncs
// intent-created and send-started strictly before it issues PostOrder, and the
// fake only fires `reached` once the request is actually on the server. So when
// the test observes `reached`, both pre-send stages are guaranteed durable and
// no confirmation can exist (the handler never returns). SIGKILL is uncatchable,
// so the CLI writes nothing further. A subsequent reconcile against a fake that
// now knows the order closes the intent out, preserving the client order_id
// from placement through the journal to the reconcile lookup.
func TestKillAfterSendThenReconcile(t *testing.T) {
	f := newFakeServer(t)
	reached := make(chan struct{}, 1)
	release := make(chan struct{})

	f.onPostOrder = func(ctx context.Context, _ *investapi.PostOrderRequest) (*investapi.PostOrderResponse, error) {
		select {
		case reached <- struct{}{}:
		default:
		}
		// Block until the client is killed (ctx cancels) or the test releases us.
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, notFound()
	}
	// After the crash, reconcile looks the order up by the client key and finds
	// it live on the broker.
	f.onGetState = func(_ context.Context, req *investapi.GetOrderStateRequest) (*investapi.OrderState, error) {
		return &investapi.OrderState{
			OrderId:               "exchange-recovered-1",
			ExecutionReportStatus: investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL,
			OrderRequestId:        req.GetOrderId(),
			LotsRequested:         1,
			LotsExecuted:          1,
		}, nil
	}

	h := newHarness(t, f.endpoint())
	h.writeConfig("")
	orderID := "33333333-3333-4333-8333-333333333333"

	cmd := h.command(placeArgs(orderID)...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatalf("start CLI: %v", err)
	}

	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		close(release)
		t.Fatalf("PostOrder never reached the fake; stderr: %s", errb.String())
	}

	if err := cmd.Process.Kill(); err != nil {
		close(release)
		t.Fatalf("kill CLI: %v", err)
	}
	_ = cmd.Wait()
	close(release)

	// Killed mid-flight: stdout must be empty or a single clean value, never noise.
	assertCleanStdout(t, out.String())

	recs := h.ledger()
	if !hasStage(recs, "intent-created") || !hasStage(recs, "send-started") {
		t.Fatalf("pre-send stages missing after crash: %v", stagesFor(recs, orderID))
	}
	for _, forbidden := range []string{"broker-confirmed", "broker-rejected", "reconciled"} {
		if hasStage(recs, forbidden) {
			t.Fatalf("unexpected %s stage after crash: %v", forbidden, stagesFor(recs, orderID))
		}
	}
	sent, ok := recordWithStage(recs, "send-started")
	if !ok || sent.OrderID != orderID {
		t.Fatalf("send-started order_id = %q, want %q", sent.OrderID, orderID)
	}

	// Recover.
	rec := h.run("orders", "reconcile", "--account", "acc-1", "-o", "json")
	if rec.exit != 0 {
		t.Fatalf("reconcile exit = %d, want 0\nstdout: %s\nstderr: %s", rec.exit, rec.stdout, rec.stderr)
	}
	env := decodeEnvelope(t, rec.stdout)
	if !env.OK {
		t.Errorf("reconcile envelope ok = false: %s", rec.stdout)
	}

	recs2 := h.ledger()
	reconciled, ok := recordWithStage(recs2, "reconciled")
	if !ok {
		t.Fatalf("no reconciled stage after reconcile: %v", stagesFor(recs2, orderID))
	}
	// order_id preserved end to end: the reconciled entry is the same intent,
	// and reconcile looked the order up by the exact client key from placement.
	if reconciled.IntentID != orderID {
		t.Errorf("reconciled intent_id = %q, want %q", reconciled.IntentID, orderID)
	}
	lookups := f.stateLookupRequests()
	if len(lookups) == 0 {
		t.Fatalf("reconcile made no GetOrderState call")
	}
	if got := lookups[0].GetOrderId(); got != orderID {
		t.Errorf("reconcile looked up order_id %q, want client key %q", got, orderID)
	}
	if lookups[0].OrderIdType == nil || *lookups[0].OrderIdType != investapi.OrderIdType_ORDER_ID_TYPE_REQUEST {
		t.Errorf("reconcile did not look up by client request id: %v", lookups[0].OrderIdType)
	}
}
