package orders

import (
	"fmt"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
)

// decimal renders a Quotation as an exact decimal string for error messages.
func decimal(q *investapi.Quotation) string {
	return render.DecimalString(q.GetUnits(), q.GetNano())
}

// Direction parses the CLI direction flag into the contract enum.
func Direction(s string) (investapi.OrderDirection, error) {
	switch s {
	case "buy":
		return investapi.OrderDirection_ORDER_DIRECTION_BUY, nil
	case "sell":
		return investapi.OrderDirection_ORDER_DIRECTION_SELL, nil
	default:
		return 0, fmt.Errorf("invalid direction %q: want buy or sell", s)
	}
}

// Type parses the CLI order-type flag into the contract enum.
func Type(s string) (investapi.OrderType, error) {
	switch s {
	case "limit":
		return investapi.OrderType_ORDER_TYPE_LIMIT, nil
	case "market":
		return investapi.OrderType_ORDER_TYPE_MARKET, nil
	case "bestprice":
		return investapi.OrderType_ORDER_TYPE_BESTPRICE, nil
	default:
		return 0, fmt.Errorf("invalid order type %q: want limit, market, or bestprice", s)
	}
}

// TimeInForce parses the CLI tif flag into the contract enum. An empty string
// yields the unspecified value, which the broker treats as DAY.
func TimeInForce(s string) (investapi.TimeInForceType, error) {
	switch s {
	case "":
		return investapi.TimeInForceType_TIME_IN_FORCE_UNSPECIFIED, nil
	case "day":
		return investapi.TimeInForceType_TIME_IN_FORCE_DAY, nil
	case "ioc":
		return investapi.TimeInForceType_TIME_IN_FORCE_FILL_AND_KILL, nil
	case "fok":
		return investapi.TimeInForceType_TIME_IN_FORCE_FILL_OR_KILL, nil
	default:
		return 0, fmt.Errorf("invalid time-in-force %q: want day, ioc, or fok", s)
	}
}

// ValidateBasics runs the order-shape checks that need no instrument data
// (plan §9): a positive lot count and a price present exactly when the order
// type is limit. It runs before any network call so bad shapes fail with
// exit 2 without a token.
func ValidateBasics(orderType investapi.OrderType, lots int64, price *investapi.Quotation) error {
	if lots <= 0 {
		return fmt.Errorf("quantity must be a positive number of lots, got %d", lots)
	}
	switch orderType {
	case investapi.OrderType_ORDER_TYPE_LIMIT:
		if price == nil {
			return fmt.Errorf("limit orders require a --price")
		}
		if price.GetUnits() < 0 || price.GetNano() < 0 {
			return fmt.Errorf("price must not be negative")
		}
		if price.GetUnits() == 0 && price.GetNano() == 0 {
			return fmt.Errorf("limit orders require a non-zero --price")
		}
	case investapi.OrderType_ORDER_TYPE_MARKET, investapi.OrderType_ORDER_TYPE_BESTPRICE:
		if price != nil {
			return fmt.Errorf("--price is not allowed for %s orders", typeName(orderType))
		}
	}
	return nil
}

// ValidatePriceIncrement checks that a limit price is an exact multiple of the
// instrument's minimum price increment (plan §9). Both are expressed as
// units+nano; comparing them in integer nano-units avoids any float error. A
// nil or zero increment (unknown for the instrument) skips the check.
func ValidatePriceIncrement(price, minIncrement *investapi.Quotation) error {
	if price == nil || minIncrement == nil {
		return nil
	}
	incr := nanoUnits(minIncrement)
	if incr == 0 {
		return nil
	}
	if incr < 0 {
		incr = -incr
	}
	p := nanoUnits(price)
	if p < 0 {
		p = -p
	}
	if p%incr != 0 {
		return fmt.Errorf("price %s is not a multiple of the instrument's min price increment %s",
			decimal(price), decimal(minIncrement))
	}
	return nil
}

// nanoUnits collapses a Quotation to a single int64 count of nano-units
// (units*1e9 + nano). Prices in this API are far from overflowing int64 at
// nano scale, so this is exact for realistic inputs.
func nanoUnits(q *investapi.Quotation) int64 {
	return q.GetUnits()*1_000_000_000 + int64(q.GetNano())
}

func typeName(t investapi.OrderType) string {
	switch t {
	case investapi.OrderType_ORDER_TYPE_MARKET:
		return "market"
	case investapi.OrderType_ORDER_TYPE_BESTPRICE:
		return "bestprice"
	default:
		return "limit"
	}
}
