package policy

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
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

// TestKillSwitchDanglingSymlinkEngaged is the F18 regression: a dangling symlink
// at the kill-switch path must engage the switch. os.Stat would follow it to a
// not-exists and treat the switch as absent; os.Lstat sees the link itself.
func TestKillSwitchDanglingSymlinkEngaged(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "KILL")
	if err := os.Symlink(filepath.Join(dir, "missing-target"), link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	p := &Policy{KillSwitchFile: link}
	if v := p.CheckKillSwitch(); v == nil {
		t.Error("a dangling kill-switch symlink must engage the switch (fail closed)")
	}
}

// TestPolicyLoadRejectsNonPositiveCaps is the F12 regression: an explicitly-set
// cap of zero or a negative value disables the guardrail silently, so Load must
// reject it. Omitting the key (no cap) stays valid.
func TestPolicyLoadRejectsNonPositiveCaps(t *testing.T) {
	bad := map[string]string{
		"negative lots":     "max_lots_per_order = -1\n",
		"zero lots":         "max_lots_per_order = 0\n",
		"negative open":     "max_open_orders = -5\n",
		"zero open":         "max_open_orders = 0\n",
		"zero notional":     "max_notional_per_order = \"0\"\nnotional_currency = \"rub\"\n",
		"negative notional": "max_notional_per_order = \"-100\"\nnotional_currency = \"rub\"\n",
	}
	for name, body := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writePolicy(t, body)); err == nil {
				t.Errorf("Load accepted a non-positive cap:\n%s", body)
			}
		})
	}
	// Omitting caps entirely is valid (no caps configured).
	if _, err := Load(writePolicy(t, "allow_market_orders = true\n")); err != nil {
		t.Errorf("Load rejected a capless policy: %v", err)
	}
}

// TestNotionalCapFailsClosedOnMissingMetadata is the F12 regression: the notional
// cap must fail closed (explicit violation) when the metadata it needs — the
// instrument's currency or lot size — is missing, rather than skipping the check
// or assuming a lot size of 1.
func TestNotionalCapFailsClosedOnMissingMetadata(t *testing.T) {
	p := &Policy{MaxNotionalPerOrder: "1000", NotionalCurrency: "rub"}
	base := OrderIntent{
		OrderType: investapi.OrderType_ORDER_TYPE_LIMIT,
		Lots:      1, Price: &investapi.Quotation{Units: 1},
		LotSize: 1, Currency: "rub", UID: "u", RawID: "u",
	}
	noCurrency := base
	noCurrency.Currency = ""
	if v := p.CheckResolved(noCurrency); v == nil {
		t.Error("notional cap must fail closed when the instrument currency is unknown")
	}
	noLot := base
	noLot.LotSize = 0
	if v := p.CheckResolved(noLot); v == nil {
		t.Error("notional cap must fail closed when the lot size is unknown")
	}
	if v := p.CheckResolved(base); v != nil {
		t.Errorf("in-cap order with complete metadata blocked: %v", v)
	}
}
