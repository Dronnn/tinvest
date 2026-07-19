//go:build e2elive

package e2elive

import (
	"encoding/json"
	"testing"
)

// instrumentArg is a cheap, liquid instrument the sandbox resolves and accepts
// limit orders for. TICKER@CLASSCODE is resolved by the CLI itself, so the
// resolution path (GetInstrumentBy) is exercised end to end.
const instrumentArg = "SBER@TQBR"

// mustData decodes an envelope's data block into v.
func mustData(t *testing.T, env envelope, v any) {
	t.Helper()
	if err := json.Unmarshal(env.Data, v); err != nil {
		t.Fatalf("decode data: %v (data=%s)", err, string(env.Data))
	}
}

// openAccount opens a fresh sandbox account and registers cleanup that fully
// closes it (cancelling any outstanding orders first), even on failure.
func (h *harness) openAccount() string {
	h.t.Helper()
	env := h.assertOK(h.runDirect("sandbox", "open", "-o", "json"), true)
	var d struct {
		AccountID string `json:"account_id"`
	}
	mustData(h.t, env, &d)
	if d.AccountID == "" {
		h.t.Fatal("sandbox open returned an empty account_id")
	}
	acc := d.AccountID
	h.t.Cleanup(func() { h.closeAccountFully(acc) })
	return acc
}

// topUp credits virtual money to a sandbox account.
func (h *harness) topUp(acc, amount string) {
	h.t.Helper()
	h.assertOK(h.runDirect("sandbox", "topup", "--account", acc, "--amount", amount, "-o", "json"), true)
}

type orderRow struct {
	OrderID       string `json:"order_id"`
	ClientOrderID string `json:"client_order_id"`
	Lifecycle     string `json:"lifecycle"`
}

// listOrders returns the account's regular orders, or nil if the read fails
// (e.g. the account is already gone) — this is used both for assertions and for
// best-effort cleanup.
func (h *harness) listOrders(acc string) []orderRow {
	res := h.runDirect("orders", "list", "--account", acc, "-o", "json")
	if res.exit != 0 {
		return nil
	}
	var env envelope
	if err := json.Unmarshal([]byte(res.stdout), &env); err != nil {
		return nil
	}
	var d struct {
		Orders []orderRow `json:"orders"`
	}
	_ = json.Unmarshal(env.Data, &d)
	return d.Orders
}

type stopRow struct {
	StopOrderID string `json:"stop_order_id"`
	Status      string `json:"status"`
}

func (h *harness) listStopOrders(acc, status string) []stopRow {
	res := h.runDirect("stop-orders", "list", "--account", acc, "--status", status, "-o", "json")
	if res.exit != 0 {
		return nil
	}
	var env envelope
	if err := json.Unmarshal([]byte(res.stdout), &env); err != nil {
		return nil
	}
	var d struct {
		StopOrders []stopRow `json:"stop_orders"`
	}
	_ = json.Unmarshal(env.Data, &d)
	return d.StopOrders
}

// cancelOutstanding cancels every active regular and stop order on the account.
// Best-effort: a cancel that races a fill or a vanished order is ignored.
func (h *harness) cancelOutstanding(acc string) {
	for _, o := range h.listOrders(acc) {
		_ = h.runDirect("orders", "cancel", o.OrderID, "--account", acc, "-o", "json")
	}
	for _, s := range h.listStopOrders(acc, "active") {
		_ = h.runDirect("stop-orders", "cancel", s.StopOrderID, "--account", acc, "-o", "json")
	}
}

// closeAccountFully cancels outstanding orders and closes the account. It never
// fails the test: cleanup must be resilient so the suite is re-runnable.
func (h *harness) closeAccountFully(acc string) {
	h.cancelOutstanding(acc)
	res := h.runDirect("sandbox", "close", acc, "-o", "json")
	if res.exit != 0 {
		h.t.Logf("cleanup: closing account %s exited %d (stderr: %s)", acc, res.exit, res.stderr)
	}
}

// sweepStrayAccounts closes every sandbox account currently visible to the
// token. Sandbox accounts are disposable, so clearing accounts orphaned by a
// previous failed run guarantees a clean slate and back-to-back re-runnability.
func (h *harness) sweepStrayAccounts() {
	res := h.runDirect("sandbox", "accounts", "-o", "json")
	if res.exit != 0 {
		h.t.Logf("stray sweep: accounts list exited %d (stderr: %s)", res.exit, res.stderr)
		return
	}
	var env envelope
	if err := json.Unmarshal([]byte(res.stdout), &env); err != nil {
		return
	}
	var d struct {
		Accounts []struct {
			ID string `json:"id"`
		} `json:"accounts"`
	}
	_ = json.Unmarshal(env.Data, &d)
	for _, a := range d.Accounts {
		h.t.Logf("sweeping stray sandbox account %s", a.ID)
		h.closeAccountFully(a.ID)
	}
}
