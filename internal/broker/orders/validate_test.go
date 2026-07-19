package orders

import (
	"testing"

	investapi "tinvest/internal/pb/investapi"
)

func q(units int64, nano int32) *investapi.Quotation {
	return &investapi.Quotation{Units: units, Nano: nano}
}

func TestValidateBasics(t *testing.T) {
	limit := investapi.OrderType_ORDER_TYPE_LIMIT
	market := investapi.OrderType_ORDER_TYPE_MARKET

	if err := ValidateBasics(limit, 0, q(1, 0)); err == nil {
		t.Error("zero lots must fail")
	}
	if err := ValidateBasics(limit, -1, q(1, 0)); err == nil {
		t.Error("negative lots must fail")
	}
	if err := ValidateBasics(limit, 1, nil); err == nil {
		t.Error("limit without price must fail")
	}
	if err := ValidateBasics(limit, 1, q(0, 0)); err != nil {
		t.Errorf("zero limit price must pass format/scale validation: %v", err)
	}
	if err := ValidateBasics(limit, 1, q(100, 0)); err != nil {
		t.Errorf("valid limit: %v", err)
	}
	if err := ValidateBasics(limit, 1, q(-10, 0)); err != nil {
		t.Errorf("negative futures limit price must be valid: %v", err)
	}
	if err := ValidateBasics(market, 1, q(100, 0)); err == nil {
		t.Error("market with price must fail")
	}
	if err := ValidateBasics(market, 1, nil); err != nil {
		t.Errorf("valid market: %v", err)
	}
}

func TestValidatePriceIncrement(t *testing.T) {
	incr := q(0, 10_000_000) // 0.01
	if err := ValidatePriceIncrement(q(100, 50_000_000), incr); err != nil {
		t.Errorf("100.05 is a multiple of 0.01: %v", err)
	}
	if err := ValidatePriceIncrement(q(100, 0), incr); err != nil {
		t.Errorf("100.00 is a multiple of 0.01: %v", err)
	}
}

func TestValidatePriceIncrementRejectsOffGrid(t *testing.T) {
	incr := q(0, 10_000_000) // 0.01
	// 100.005 is not a multiple of 0.01.
	if err := ValidatePriceIncrement(q(100, 5_000_000), incr); err == nil {
		t.Error("100.005 is off the 0.01 grid, must fail")
	}
}

func TestValidatePriceIncrementNilSkips(t *testing.T) {
	if err := ValidatePriceIncrement(nil, q(0, 10_000_000)); err != nil {
		t.Errorf("nil price skips: %v", err)
	}
	if err := ValidatePriceIncrement(q(1, 0), nil); err != nil {
		t.Errorf("nil increment skips: %v", err)
	}
	if err := ValidatePriceIncrement(q(1, 0), q(0, 0)); err != nil {
		t.Errorf("zero increment skips: %v", err)
	}
}

func TestDirectionTypeTIFParse(t *testing.T) {
	if d, err := Direction("buy"); err != nil || d != investapi.OrderDirection_ORDER_DIRECTION_BUY {
		t.Errorf("buy: %v %v", d, err)
	}
	if _, err := Direction("hold"); err == nil {
		t.Error("invalid direction must fail")
	}
	if ot, err := Type("bestprice"); err != nil || ot != investapi.OrderType_ORDER_TYPE_BESTPRICE {
		t.Errorf("bestprice: %v %v", ot, err)
	}
	if _, err := Type("iceberg"); err == nil {
		t.Error("invalid type must fail")
	}
	if tif, err := TimeInForce("ioc"); err != nil || tif != investapi.TimeInForceType_TIME_IN_FORCE_FILL_AND_KILL {
		t.Errorf("ioc: %v %v", tif, err)
	}
	if tif, err := TimeInForce(""); err != nil || tif != investapi.TimeInForceType_TIME_IN_FORCE_UNSPECIFIED {
		t.Errorf("empty tif: %v %v", tif, err)
	}
	if _, err := TimeInForce("gtc"); err == nil {
		t.Error("invalid tif must fail")
	}
}

func TestLifecycleAndTerminal(t *testing.T) {
	cases := []struct {
		s        investapi.OrderExecutionReportStatus
		name     string
		terminal bool
	}{
		{investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW, LifecycleNew, false},
		{investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_PARTIALLYFILL, LifecyclePartiallyFilled, false},
		{investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL, LifecycleFilled, true},
		{investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED, LifecycleCancelled, true},
		{investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_REJECTED, LifecycleRejected, true},
	}
	for _, c := range cases {
		if got := Lifecycle(c.s); got != c.name {
			t.Errorf("Lifecycle(%v) = %q, want %q", c.s, got, c.name)
		}
		if got := IsTerminal(c.s); got != c.terminal {
			t.Errorf("IsTerminal(%v) = %v, want %v", c.s, got, c.terminal)
		}
	}
}
