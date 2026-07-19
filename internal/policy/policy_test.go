package policy

import (
	"os"
	"path/filepath"
	"testing"

	investapi "tinvest/internal/pb/investapi"
)

func limitIntent(lots int64, priceUnits int64) OrderIntent {
	return OrderIntent{
		Direction: investapi.OrderDirection_ORDER_DIRECTION_BUY,
		OrderType: investapi.OrderType_ORDER_TYPE_LIMIT,
		Lots:      lots,
		Price:     &investapi.Quotation{Units: priceUnits},
		LotSize:   1,
		Currency:  "rub",
		UID:       "uid-1",
		RawID:     "uid-1",
	}
}

func clearHomeEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"HOME", "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "home"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLoadRejectsUnknownKey(t *testing.T) {
	path := writePolicy(t, "max_lots_per_order = 5\nbogus_key = true\n")
	if _, err := Load(path); err == nil {
		t.Fatal("want error for unknown key")
	}
}

func TestLoadMissingFileIsError(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.toml")); err == nil {
		t.Fatal("want error for missing policy file")
	}
}

func TestLoadEmptyPathIsPermissive(t *testing.T) {
	p, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if p != nil {
		t.Fatalf("want nil policy, got %+v", p)
	}
	// nil policy waves everything through.
	if v := p.CheckLocal(limitIntent(1000, 1)); v != nil {
		t.Errorf("nil policy should allow, got %v", v)
	}
}

func TestKillSwitchBlocks(t *testing.T) {
	kill := filepath.Join(t.TempDir(), "KILL")
	if err := os.WriteFile(kill, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := &Policy{KillSwitchFile: kill, present: true}
	v := p.CheckLocal(limitIntent(1, 1))
	if v == nil || v.Rule != "kill_switch" {
		t.Fatalf("want kill_switch violation, got %+v", v)
	}
}

func TestKillSwitchAbsentAllows(t *testing.T) {
	p := &Policy{KillSwitchFile: filepath.Join(t.TempDir(), "absent"), present: true}
	if v := p.CheckKillSwitch(); v != nil {
		t.Fatalf("absent kill switch must not block, got %v", v)
	}
}

func TestMarketOrdersDisabledByDefault(t *testing.T) {
	p := &Policy{present: true} // AllowMarketOrders false
	in := limitIntent(1, 1)
	in.OrderType = investapi.OrderType_ORDER_TYPE_MARKET
	in.Price = nil
	v := p.CheckLocal(in)
	if v == nil || v.Rule != "allow_market_orders" {
		t.Fatalf("want allow_market_orders violation, got %+v", v)
	}
}

func TestMarketOrdersAllowedWhenOptedIn(t *testing.T) {
	p := &Policy{present: true, AllowMarketOrders: true}
	in := limitIntent(1, 1)
	in.OrderType = investapi.OrderType_ORDER_TYPE_MARKET
	in.Price = nil
	if v := p.CheckLocal(in); v != nil {
		t.Fatalf("market allowed, got %v", v)
	}
}

func TestMaxLotsPerOrder(t *testing.T) {
	p := &Policy{present: true, MaxLotsPerOrder: 10}
	if v := p.CheckLocal(limitIntent(10, 1)); v != nil {
		t.Fatalf("10 lots is at the cap, must pass: %v", v)
	}
	v := p.CheckLocal(limitIntent(11, 1))
	if v == nil || v.Rule != "max_lots_per_order" {
		t.Fatalf("want max_lots_per_order violation, got %+v", v)
	}
	if v.Details["limit"] != "10" || v.Details["actual"] != "11" {
		t.Errorf("details = %v", v.Details)
	}
}

func TestAllowlist(t *testing.T) {
	p := &Policy{AllowedInstruments: []string{"uid-allowed", "BBG000000001"}}
	blocked := limitIntent(1, 1) // uid-1, not in list
	v := p.CheckResolved(blocked)
	if v == nil || v.Rule != "allowed_instruments" {
		t.Fatalf("want allowlist violation, got %+v", v)
	}
	ok := limitIntent(1, 1)
	ok.UID = "uid-allowed"
	if v := p.CheckResolved(ok); v != nil {
		t.Fatalf("allowlisted uid must pass, got %v", v)
	}
	// Match by FIGI too.
	byFigi := limitIntent(1, 1)
	byFigi.UID = "uid-x"
	byFigi.FIGI = "BBG000000001"
	if v := p.CheckResolved(byFigi); v != nil {
		t.Fatalf("allowlisted figi must pass, got %v", v)
	}
}

func TestNotionalCap(t *testing.T) {
	// max 1000 rub; lot size 10, price 5 => notional 50/lot.
	p := &Policy{MaxNotionalPerOrder: "1000", NotionalCurrency: "rub"}
	in := limitIntent(20, 5) // 20 * 10 * 5 = 1000, at the cap
	in.LotSize = 10
	if v := p.CheckResolved(in); v != nil {
		t.Fatalf("notional at cap must pass, got %v", v)
	}
	over := limitIntent(21, 5) // 21 * 10 * 5 = 1050 > 1000
	over.LotSize = 10
	v := p.CheckResolved(over)
	if v == nil || v.Rule != "max_notional_per_order" {
		t.Fatalf("want notional violation, got %+v", v)
	}
}

func TestNotionalCurrencyMismatch(t *testing.T) {
	p := &Policy{MaxNotionalPerOrder: "1000", NotionalCurrency: "rub"}
	in := limitIntent(1, 5)
	in.Currency = "usd"
	v := p.CheckResolved(in)
	if v == nil || v.Rule != "max_notional_per_order" {
		t.Fatalf("want currency-mismatch violation, got %+v", v)
	}
}

func TestNotionalSkippedForMarket(t *testing.T) {
	p := &Policy{MaxNotionalPerOrder: "1", NotionalCurrency: "rub"}
	in := limitIntent(1000000, 0)
	in.Price = nil // market, no price
	if v := p.CheckResolved(in); v != nil {
		t.Fatalf("notional must be skipped for market (no price), got %v", v)
	}
}

func TestMaxOpenOrders(t *testing.T) {
	p := &Policy{MaxOpenOrders: 3}
	if v := p.CheckOpenOrders(2); v != nil {
		t.Fatalf("2 < 3 must pass, got %v", v)
	}
	v := p.CheckOpenOrders(3)
	if v == nil || v.Rule != "max_open_orders" {
		t.Fatalf("want max_open_orders violation at the cap, got %+v", v)
	}
}

func TestCheckKillSwitchFailsClosedWhenHomeCannotBeResolved(t *testing.T) {
	clearHomeEnv(t)

	p := &Policy{KillSwitchFile: "~/tinvest-kill-switch"}
	if v := p.CheckKillSwitch(); v == nil {
		t.Fatal("unexpandable kill_switch_file was treated as absent")
	}
}

func TestLoadRejectsUnexpandableKillSwitchPath(t *testing.T) {
	clearHomeEnv(t)
	path := writePolicy(t, "kill_switch_file = \"~/tinvest-kill-switch\"\n")

	if _, err := Load(path); err == nil {
		t.Fatal("policy with an unexpandable kill_switch_file was accepted")
	}
}

func writePolicy(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
