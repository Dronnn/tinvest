package stoporders

import (
	"testing"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

func q(units int64, nano int32) *investapi.Quotation {
	return &investapi.Quotation{Units: units, Nano: nano}
}

func baseBasics() BasicsInput {
	return BasicsInput{
		StopOrderType:  investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
		Quantity:       1,
		StopPrice:      q(100, 0),
		ExpirationType: investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
	}
}

func TestValidateBasicsQuantityAndStopPrice(t *testing.T) {
	in := baseBasics()
	in.Quantity = 0
	if err := ValidateBasics(in); err == nil {
		t.Error("zero quantity must fail")
	}

	in = baseBasics()
	in.StopPrice = nil
	if err := ValidateBasics(in); err == nil {
		t.Error("missing stop-price must fail")
	}

	in = baseBasics()
	in.StopPrice = q(0, 0)
	if err := ValidateBasics(in); err == nil {
		t.Error("zero stop-price must fail")
	}

	if err := ValidateBasics(baseBasics()); err != nil {
		t.Errorf("valid stop-loss: %v", err)
	}
}

func TestValidateBasicsPriceOnlyForStopLimit(t *testing.T) {
	in := baseBasics()
	in.StopOrderType = investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT
	in.Price = q(100, 0)
	if err := ValidateBasics(in); err == nil {
		t.Error("price on a take-profit order must fail")
	}

	in.StopOrderType = investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS
	if err := ValidateBasics(in); err == nil {
		t.Error("price on a stop-loss order must fail")
	}

	in.StopOrderType = investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT
	if err := ValidateBasics(in); err != nil {
		t.Errorf("valid stop-limit with price: %v", err)
	}

	in.Price = nil
	if err := ValidateBasics(in); err == nil {
		t.Error("stop-limit without price must fail")
	}
}

// TestValidateBasicsRequiresExpireDateForGTD is one of the required M1e
// tests: expiration GTD without --expire-date must fail, and with it must
// pass.
func TestValidateBasicsRequiresExpireDateForGTD(t *testing.T) {
	in := baseBasics()
	in.ExpirationType = investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE
	in.HasExpireDate = false
	if err := ValidateBasics(in); err == nil {
		t.Fatal("gtd without expire_date must fail")
	}

	in.HasExpireDate = true
	if err := ValidateBasics(in); err != nil {
		t.Errorf("gtd with expire_date must pass: %v", err)
	}
}

func TestValidateBasicsRejectsExpireDateForGTC(t *testing.T) {
	in := baseBasics()
	in.HasExpireDate = true // GTC (the default in baseBasics) with an expire_date is nonsensical
	if err := ValidateBasics(in); err == nil {
		t.Error("gtc with expire_date must fail")
	}
}

// TestValidateBasicsTrailingOnlyForTakeProfit is one of the required M1e
// tests: trailing indent/spread fields are only valid for take-profit orders.
func TestValidateBasicsTrailingOnlyForTakeProfit(t *testing.T) {
	trailing := &TrailingParams{
		Indent: q(1, 0), IndentType: investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE,
		Spread: q(1, 0), SpreadType: investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE,
	}

	in := baseBasics() // stop-loss
	in.Trailing = trailing
	if err := ValidateBasics(in); err == nil {
		t.Fatal("trailing fields on a stop-loss order must fail")
	}

	in.StopOrderType = investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT
	if err := ValidateBasics(in); err != nil {
		t.Errorf("trailing fields on a take-profit order must pass: %v", err)
	}
}

func TestValidateBasicsTakeProfitTypeTrailingRequiresTrailingFields(t *testing.T) {
	in := baseBasics()
	in.StopOrderType = investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT
	in.TakeProfitType = investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING
	if err := ValidateBasics(in); err == nil {
		t.Error("take-profit-type trailing without trailing fields must fail")
	}
}

func TestValidateBasicsRejectsTakeProfitTypeForOtherStopTypes(t *testing.T) {
	for _, stopType := range []investapi.StopOrderType{
		investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
		investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT,
	} {
		in := baseBasics()
		in.StopOrderType = stopType
		if stopType == investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT {
			in.Price = q(99, 0)
		}
		in.TakeProfitType = investapi.TakeProfitType_TAKE_PROFIT_TYPE_REGULAR
		if err := ValidateBasics(in); err == nil {
			t.Errorf("take-profit-type on %v must fail", stopType)
		}
	}
}

func TestValidateBasicsRejectsExchangeOrderTypeForOtherStopTypes(t *testing.T) {
	for _, stopType := range []investapi.StopOrderType{
		investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS,
		investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT,
	} {
		in := baseBasics()
		in.StopOrderType = stopType
		if stopType == investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT {
			in.Price = q(99, 0)
		}
		in.ExchangeOrderType = investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_MARKET
		if err := ValidateBasics(in); err == nil {
			t.Errorf("exchange-order-type on %v must fail", stopType)
		}
	}
}

func TestDirectionTypeExpirationParse(t *testing.T) {
	if d, err := Direction("buy"); err != nil || d != investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY {
		t.Errorf("buy: %v %v", d, err)
	}
	if _, err := Direction("hold"); err == nil {
		t.Error("invalid direction must fail")
	}
	if ty, err := Type("stop-limit"); err != nil || ty != investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT {
		t.Errorf("stop-limit: %v %v", ty, err)
	}
	if _, err := Type("iceberg"); err == nil {
		t.Error("invalid type must fail")
	}
	if e, err := Expiration(""); err != nil || e != investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL {
		t.Errorf("default expiration: %v %v", e, err)
	}
	if e, err := Expiration("gtd"); err != nil || e != investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE {
		t.Errorf("gtd: %v %v", e, err)
	}
	if _, err := Expiration("forever"); err == nil {
		t.Error("invalid expiration must fail")
	}
}

func TestStatusName(t *testing.T) {
	cases := []struct {
		s    investapi.StopOrderStatusOption
		want string
	}{
		{investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ACTIVE, StatusActive},
		{investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXECUTED, StatusExecuted},
		{investapi.StopOrderStatusOption_STOP_ORDER_STATUS_CANCELED, StatusCanceled},
		{investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXPIRED, StatusExpired},
		{investapi.StopOrderStatusOption_STOP_ORDER_STATUS_UNSPECIFIED, StatusUnspecified},
	}
	for _, c := range cases {
		if got := StatusName(c.s); got != c.want {
			t.Errorf("StatusName(%v) = %q, want %q", c.s, got, c.want)
		}
	}
}
