package policy

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	investapi "tinvest/internal/pb/investapi"
)

// TestKillSwitchFailsClosedOnStatError is the F11(a) regression: a stat error
// other than not-exists (here ENOTDIR, from a path whose parent is a regular
// file) must BLOCK the mutation, not wave it through. Fails on 4304f5a.
func TestKillSwitchFailsClosedOnStatError(t *testing.T) {
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := &Policy{KillSwitchFile: filepath.Join(notDir, "kill")}
	v := p.CheckKillSwitch()
	if v == nil {
		t.Fatal("kill switch failed OPEN on a stat error; it must fail closed")
	}
	if v.Rule != "kill_switch" {
		t.Errorf("rule = %q, want kill_switch", v.Rule)
	}
}

// TestKillSwitchNormalCases confirms the fail-closed change did not disturb the
// ordinary engaged / not-engaged / unconfigured behavior.
func TestKillSwitchNormalCases(t *testing.T) {
	dir := t.TempDir()
	kill := filepath.Join(dir, "KILL")
	p := &Policy{KillSwitchFile: kill}
	if v := p.CheckKillSwitch(); v != nil {
		t.Errorf("absent kill file must not block: %v", v)
	}
	if err := os.WriteFile(kill, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if v := p.CheckKillSwitch(); v == nil {
		t.Error("present kill file must block")
	}
	if v := (&Policy{}).CheckKillSwitch(); v != nil {
		t.Errorf("unconfigured kill switch must never block: %v", v)
	}
}

// TestNotionalCapNoInt64Overflow is the F11(c) regression: lots*lotSize is done
// in big arithmetic, so an enormous order cannot wrap int64 and slip under the
// cap. Fails on 4304f5a, where the overflow made the notional negative.
func TestNotionalCapNoInt64Overflow(t *testing.T) {
	p := &Policy{MaxNotionalPerOrder: "1000000", NotionalCurrency: "rub"}
	in := limitIntent(math.MaxInt64, 1) // price 1 rub, MaxInt64 lots
	in.LotSize = 2                      // MaxInt64 * 2 overflows int64

	v := p.CheckResolved(in)
	if v == nil {
		t.Fatal("int64 overflow let an enormous order bypass max_notional")
	}
	if v.Rule != "max_notional_per_order" {
		t.Errorf("rule = %q, want max_notional_per_order", v.Rule)
	}
	// A genuinely in-cap order still passes.
	if v := p.CheckResolved(limitIntent(1, 1)); v != nil {
		t.Errorf("in-cap order incorrectly blocked: %v", v)
	}
}

// TestMarketOrderBypassesNotionalCap documents the F11(c) market-order bypass:
// a market order has no price at the local validation stage, so max_notional
// cannot apply — allow_market_orders is its guard. A market order must not be
// blocked by the notional cap even when the cap is tiny.
func TestMarketOrderBypassesNotionalCap(t *testing.T) {
	p := &Policy{MaxNotionalPerOrder: "1", NotionalCurrency: "rub"}
	in := OrderIntent{
		OrderType: investapi.OrderType_ORDER_TYPE_MARKET,
		Lots:      1_000_000,
		Price:     nil, // market: unpriced locally
		LotSize:   1,
		Currency:  "rub",
		UID:       "uid-1",
		RawID:     "uid-1",
	}
	if v := p.CheckResolved(in); v != nil {
		t.Errorf("market order blocked by notional cap, want documented bypass: %v", v)
	}
}
