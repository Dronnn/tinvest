//go:build e2elive

package e2elive

import "testing"

// TestSandboxLifecycle drives one full order lifecycle DIRECT against the real
// sandbox: open -> topup -> place -> get -> list -> replace -> cancel ->
// stop-orders place/list/cancel -> reconcile -> close. Every step asserts the
// exit code and the JSON envelope shape (ok:true, meta.tracking_id on broker
// round-trips). Because MOEX is closed at weekends the market never fills these
// orders — they rest as accepted ("new"), which is what the suite asserts. No
// fills are asserted anywhere.
func TestSandboxLifecycle(t *testing.T) {
	requireLive(t)
	h := newHarness(t)
	h.sweepStrayAccounts()

	acc := h.openAccount()
	h.topUp(acc, "1000000")

	// --- place a resting limit order far below the market ---
	placeID := newOrderID(t)
	placeEnv := h.assertOK(h.runDirect(
		"orders", "place",
		"--account", acc,
		"--instrument", instrumentArg,
		"--direction", "buy",
		"--quantity", "1",
		"--type", "limit",
		"--price", "10",
		"--order-id", placeID,
		"--yes", "--no-cache",
		"-o", "json",
	), true)
	var placed struct {
		Order struct {
			OrderID       string `json:"order_id"`
			ClientOrderID string `json:"client_order_id"`
			Lifecycle     string `json:"lifecycle"`
		} `json:"order"`
	}
	mustData(t, placeEnv, &placed)
	if placed.Order.ClientOrderID != placeID {
		t.Fatalf("client_order_id = %q, want %q", placed.Order.ClientOrderID, placeID)
	}
	if placed.Order.OrderID == "" {
		t.Fatalf("place returned an empty exchange order_id: %s", placeEnv.Data)
	}
	exID := placed.Order.OrderID

	// The write-ahead ledger drove a clean placement to a confirmed terminal.
	recs := h.ledger()
	for _, stage := range []string{"intent-created", "send-started", "broker-confirmed"} {
		if !hasStageFor(recs, placeID, stage) {
			t.Fatalf("ledger missing %s for %s: %v", stage, placeID, stagesFor(recs, placeID))
		}
	}

	// --- read it back by exchange id and by client (request) id ---
	getEnv := h.assertOK(h.runDirect("orders", "get", exID, "--account", acc, "-o", "json"), true)
	var got struct {
		Order struct {
			OrderID   string `json:"order_id"`
			Lifecycle string `json:"lifecycle"`
		} `json:"order"`
	}
	mustData(t, getEnv, &got)
	if got.Order.OrderID != exID {
		t.Errorf("orders get returned order_id %q, want %q", got.Order.OrderID, exID)
	}
	if got.Order.Lifecycle == "" {
		t.Errorf("orders get returned an empty lifecycle: %s", getEnv.Data)
	}
	h.assertOK(h.runDirect("orders", "get", placeID, "--account", acc, "--request-id", "-o", "json"), true)

	// --- it appears in the list under its client id ---
	if !containsOrderWithClientID(h.listOrders(acc), placeID) {
		t.Errorf("orders list does not contain the placed order (client_order_id %s)", placeID)
	}

	// --- replace it (new quantity, explicit price) ---
	replaceEnv := h.assertOK(h.runDirect(
		"orders", "replace", exID,
		"--account", acc,
		"--quantity", "2",
		"--price", "10",
		"-o", "json",
	), true)
	var replaced struct {
		Order struct {
			OrderID string `json:"order_id"`
			Lots    struct {
				Requested int64 `json:"requested"`
			} `json:"lots"`
		} `json:"order"`
	}
	mustData(t, replaceEnv, &replaced)
	if replaced.Order.OrderID == "" {
		t.Fatalf("replace returned an empty order_id: %s", replaceEnv.Data)
	}
	if replaced.Order.Lots.Requested != 2 {
		t.Errorf("replaced order requested lots = %d, want 2", replaced.Order.Lots.Requested)
	}
	replacedID := replaced.Order.OrderID

	// --- cancel the replaced order; the book is then empty ---
	h.assertOK(h.runDirect("orders", "cancel", replacedID, "--account", acc, "-o", "json"), true)
	if got := len(h.listOrders(acc)); got != 0 {
		t.Errorf("after cancel, active regular orders = %d, want 0", got)
	}

	// --- a stop-loss far from market: placed, listed, cancelled ---
	stopID := newOrderID(t)
	stopEnv := h.assertOK(h.runDirect(
		"stop-orders", "place",
		"--account", acc,
		"--instrument", instrumentArg,
		"--direction", "sell",
		"--quantity", "1",
		"--type", "stop-loss",
		"--stop-price", "1",
		"--order-id", stopID,
		"--yes", "--no-cache",
		"-o", "json",
	), true)
	var stop struct {
		StopOrder struct {
			StopOrderID string `json:"stop_order_id"`
		} `json:"stop_order"`
	}
	mustData(t, stopEnv, &stop)
	if stop.StopOrder.StopOrderID == "" {
		t.Fatalf("stop-orders place returned an empty stop_order_id: %s", stopEnv.Data)
	}
	stopExID := stop.StopOrder.StopOrderID
	if !containsStop(h.listStopOrders(acc, "active"), stopExID) {
		t.Errorf("stop-orders list (active) does not contain %s", stopExID)
	}
	h.assertOK(h.runDirect("stop-orders", "cancel", stopExID, "--account", acc, "-o", "json"), true)

	// --- reconcile must converge: exit 0, nothing left in doubt ---
	recRes := h.runDirect("orders", "reconcile", "--account", acc, "-o", "json")
	if recRes.exit != 0 {
		t.Fatalf("reconcile exit = %d, want 0\nstdout: %s\nstderr: %s", recRes.exit, recRes.stdout, recRes.stderr)
	}
	recEnv := decodeEnvelope(t, recRes.stdout)
	if !recEnv.OK {
		t.Errorf("reconcile envelope ok = false: %s", recRes.stdout)
	}
	var recData struct {
		UnresolvedCount int `json:"unresolved_count"`
	}
	mustData(t, recEnv, &recData)
	if recData.UnresolvedCount != 0 {
		t.Errorf("reconcile left %d unresolved intents, want 0: %s", recData.UnresolvedCount, recEnv.Data)
	}

	// --- close (cleanup also closes; a second close is a harmless no-op) ---
	h.assertOK(h.runDirect("sandbox", "close", acc, "-o", "json"), true)
}

func containsOrderWithClientID(orders []orderRow, clientID string) bool {
	for _, o := range orders {
		if o.ClientOrderID == clientID {
			return true
		}
	}
	return false
}

func containsStop(stops []stopRow, stopOrderID string) bool {
	for _, s := range stops {
		if s.StopOrderID == stopOrderID {
			return true
		}
	}
	return false
}
