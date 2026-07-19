package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/policy"
	"tinvest/internal/render"
)

const testUUID = "00000000-0000-4000-8000-000000000001"
const testEndpoint = "passthrough:///bufnet"

func TestOrderBuildersRejectNonUUIDOrderIDs(t *testing.T) {
	_, cerr := buildPlace(placeInput{
		Instrument: testUUID, Direction: "buy", Quantity: 1, Type: "market", OrderID: "not-a-uuid",
	})
	if cerr == nil {
		t.Fatal("orders place accepted a non-UUID order_id")
	}

	_, cerr = buildStopPlace(stopPlaceInput{
		Instrument: testUUID, Direction: "buy", Quantity: 1, Type: "stop-loss",
		StopPrice: "100", OrderID: "not-a-uuid",
	})
	if cerr == nil {
		t.Fatal("stop-orders place accepted a non-UUID order_id")
	}
}

func TestOrdersPlaceAndReplaceExposeConfirmMarginTrade(t *testing.T) {
	a := &app{}
	if a.ordersPlaceCmd().Flags().Lookup("confirm-margin-trade") == nil {
		t.Error("orders place is missing --confirm-margin-trade")
	}
	if a.ordersReplaceCmd().Flags().Lookup("confirm-margin-trade") == nil {
		t.Error("orders replace is missing --confirm-margin-trade")
	}
}

func TestPlaceTableRendersStandalonePreviewAndMaxLots(t *testing.T) {
	tests := []struct {
		name string
		data placeData
		want string
	}{
		{
			name: "preview",
			data: placeData{Preview: &render.PreviewView{
				LotsRequested: 2,
				TotalAmount:   &render.Decimal{Value: "250", Currency: "RUB"},
			}},
			want: "lots_requested",
		},
		{
			name: "max lots",
			data: placeData{MaxLots: &render.MaxLotsView{
				Currency: "rub", BuyMaxLots: 42, BuyMaxMarketLot: 40, SellMaxLots: 17,
			}},
			want: "buy_max_lots",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := placeTable(&out, tt.data); err != nil {
				t.Fatalf("placeTable: %v", err)
			}
			if !strings.Contains(out.String(), tt.want) {
				t.Fatalf("table output %q does not contain %q", out.String(), tt.want)
			}
		})
	}
}

func TestCancelUnconfirmedErrorGetsActionableReconcileHint(t *testing.T) {
	for _, tt := range []struct {
		name    string
		orderID string
		command string
	}{
		{name: "order", orderID: "exchange-order-id", command: "tinvest orders get exchange-order-id"},
		{name: "stop order", orderID: "stop-order-id", command: "tinvest stop-orders list --status all"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cerr := addCancelReconcileHint(&render.CLIError{Code: render.CodeUnconfirmed}, tt.orderID, tt.command)
			if cerr.ReconcileHint == nil {
				t.Fatal("missing reconcile hint")
			}
			if cerr.ReconcileHint.OrderID != tt.orderID || cerr.ReconcileHint.Command != tt.command {
				t.Errorf("reconcile hint = %+v", cerr.ReconcileHint)
			}
		})
	}
}

func TestBuildPlaceCarriesConfirmMarginTrade(t *testing.T) {
	rp, cerr := buildPlace(placeInput{
		Instrument: testUUID, Direction: "buy", Quantity: 1, Type: "market",
		OrderID: testUUID, ConfirmMarginTrade: true,
	})
	if cerr != nil {
		t.Fatalf("buildPlace: %+v", cerr)
	}
	if !rp.confirmMarginTrade {
		t.Error("confirm_margin_trade was not carried into resolvedPlace")
	}
}

func TestReplacementPolicyChecksLotNotionalAndInstrumentCaps(t *testing.T) {
	policyFile := t.TempDir() + "/policy.toml"
	write := `allowed_instruments = ["allowed-uid"]
max_lots_per_order = 2
max_notional_per_order = "100"
notional_currency = "rub"
allow_market_orders = true
`
	if err := os.WriteFile(policyFile, []byte(write), 0o600); err != nil {
		t.Fatal(err)
	}
	pol, err := policy.Load(policyFile)
	if err != nil {
		t.Fatal(err)
	}
	state := &investapi.OrderState{
		Direction:            investapi.OrderDirection_ORDER_DIRECTION_BUY,
		OrderType:            investapi.OrderType_ORDER_TYPE_LIMIT,
		LotsRequested:        10,
		InitialOrderPrice:    &investapi.MoneyValue{Units: 500, Currency: "rub"},
		InitialSecurityPrice: &investapi.MoneyValue{Units: 50, Currency: "rub"},
		InstrumentUid:        "allowed-uid",
	}
	inst := &investapi.Instrument{Uid: "allowed-uid", Lot: 1, Currency: "rub"}

	if v := replacementPolicyViolation(pol, 3, nil, state, inst); v == nil || v.Rule != "max_lots_per_order" {
		t.Fatalf("lot-cap violation = %+v", v)
	}
	if v := replacementPolicyViolation(pol, 2, &investapi.Quotation{Units: 60}, state, inst); v == nil || v.Rule != "max_notional_per_order" {
		t.Fatalf("notional-cap violation = %+v", v)
	}
	if v := replacementPolicyViolation(pol, 2, nil, state, inst); v != nil {
		t.Fatalf("unchanged per-instrument price should be at the cap, violation = %+v", v)
	}
	if v := replacementPolicyViolation(pol, 2, &investapi.Quotation{Units: -60}, state, inst); v == nil || v.Rule != "max_notional_per_order" {
		t.Fatalf("negative-price notional-cap violation = %+v", v)
	}
	inst.Uid = "blocked-uid"
	state.InstrumentUid = "blocked-uid"
	if v := replacementPolicyViolation(pol, 1, nil, state, inst); v == nil || v.Rule != "allowed_instruments" {
		t.Fatalf("allowlist violation = %+v", v)
	}
}
