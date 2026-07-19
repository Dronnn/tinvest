//go:build e2elive

package e2elive

import (
	"bytes"
	"testing"
	"time"
)

// TestKillAfterSendThenReconcileLive is the crash-injection case against the
// REAL sandbox. A transparent loopback relay (proxy_test.go) forwards placement
// to the sandbox and fires its hook the instant the broker's PostOrder response
// arrives — the order is by then created on the sandbox, but the response has
// not been relayed to the CLI. The test SIGKILLs the CLI at that instant, so the
// journal holds intent-created and send-started (both fsynced before the wire
// send) but no confirmation. `orders reconcile` then converges the intent to the
// truth on the sandbox, and a direct sandbox read confirms exactly one order
// exists for the idempotency key.
//
// The timing is deterministic, not raced: send-started is durable before the CLI
// issues PostOrder, and the hook only fires once the sandbox has answered, then
// blocks so the response can never reach the CLI before the kill lands. SIGKILL
// is uncatchable, so the CLI writes nothing further.
//
// Reconcile runs through the same relay rather than a fresh direct connection:
// the product binds each intent to the exact endpoint recorded at placement
// (reconcileTargetMismatch in cmd/tinvest/orders.go), so reconciliation must use
// the endpoint the placement used. The relay is a byte-for-byte passthrough, so
// the truth still comes from the real sandbox; the independent no-duplicate check
// below issues a genuinely direct sandbox read with no relay in the path.
func TestKillAfterSendThenReconcileLive(t *testing.T) {
	requireLive(t)
	h := newHarness(t)
	h.sweepStrayAccounts()

	acc := h.openAccount()
	h.topUp(acc, "1000000")

	rl := newRelay(t)
	h.writeRelayConfig(rl.addr)

	reached := make(chan struct{}, 1)
	release := make(chan struct{})
	rl.setOnPostOrder(func() {
		reached <- struct{}{}
		<-release // hold the broker's response until the CLI has been killed
	})

	placeID := newOrderID(t)
	cmd := h.relayCommand(
		"orders", "place",
		"--account", acc,
		"--instrument", instrumentArg,
		"--direction", "buy",
		"--quantity", "1",
		"--type", "limit",
		"--price", "10",
		"--order-id", placeID,
		"--yes", "--no-cache",
		"--timeout", "30s",
		"-o", "json",
	)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatalf("start CLI: %v", err)
	}

	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		close(release)
		t.Fatalf("PostOrder never reached the sandbox through the relay; stderr: %s", errb.String())
	}

	if err := cmd.Process.Kill(); err != nil {
		close(release)
		t.Fatalf("kill CLI: %v", err)
	}
	_ = cmd.Wait()
	close(release)
	rl.setOnPostOrder(nil) // subsequent relay traffic (reconcile) is fully transparent

	// Killed mid-flight: stdout is empty or a single clean value, never noise.
	assertCleanStdout(t, out.String())

	// The write-ahead stages are durable; nothing past the send was recorded.
	recs := h.ledger()
	if !hasStageFor(recs, placeID, "intent-created") || !hasStageFor(recs, placeID, "send-started") {
		t.Fatalf("pre-send stages missing after crash: %v", stagesFor(recs, placeID))
	}
	for _, forbidden := range []string{"broker-confirmed", "broker-rejected", "reconciled"} {
		if hasStageFor(recs, placeID, forbidden) {
			t.Fatalf("unexpected %s stage after crash: %v", forbidden, stagesFor(recs, placeID))
		}
	}

	// --- reconcile converges the stuck intent to the sandbox's truth ---
	recRes := h.runRelay("orders", "reconcile", "--account", acc, "-o", "json")
	if recRes.exit != 0 {
		t.Fatalf("reconcile exit = %d, want 0\nstdout: %s\nstderr: %s", recRes.exit, recRes.stdout, recRes.stderr)
	}
	recEnv := decodeEnvelope(t, recRes.stdout)
	if !recEnv.OK {
		t.Errorf("reconcile envelope ok = false: %s", recRes.stdout)
	}
	var recData struct {
		Outcomes []struct {
			IntentID string `json:"intent_id"`
			Outcome  string `json:"outcome"`
			OrderID  string `json:"order_id"`
		} `json:"outcomes"`
		UnresolvedCount int `json:"unresolved_count"`
	}
	mustData(t, recEnv, &recData)
	if recData.UnresolvedCount != 0 {
		t.Errorf("reconcile left %d unresolved intents, want 0: %s", recData.UnresolvedCount, recEnv.Data)
	}
	var resolved bool
	for _, o := range recData.Outcomes {
		if o.IntentID != placeID {
			continue
		}
		resolved = true
		if o.Outcome != "placed" {
			t.Errorf("intent %s outcome = %q, want placed: %s", placeID, o.Outcome, recEnv.Data)
		}
		if o.OrderID == "" {
			t.Errorf("resolved intent %s carries no exchange order_id: %s", placeID, recEnv.Data)
		}
	}
	if !resolved {
		t.Fatalf("intent %s not present in reconcile outcomes: %s", placeID, recEnv.Data)
	}

	// The ledger now ends in a truthful terminal state for this intent.
	if !hasStageFor(h.ledger(), placeID, "reconciled") {
		t.Errorf("intent %s has no reconciled stage after reconcile: %v", placeID, stagesFor(h.ledger(), placeID))
	}

	// --- direct sandbox read (no relay): exactly one order for the key ---
	n := 0
	for _, o := range h.listOrders(acc) {
		if o.ClientOrderID == placeID {
			n++
		}
	}
	if n != 1 {
		t.Errorf("direct sandbox list shows %d orders for idempotency key %s, want exactly 1 (no duplicate)", n, placeID)
	}
	h.assertOK(h.runDirect("orders", "get", placeID, "--account", acc, "--request-id", "-o", "json"), true)
}
