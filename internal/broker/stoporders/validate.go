package stoporders

import (
	"fmt"

	investapi "tinvest/internal/pb/investapi"
)

// Direction parses the CLI direction flag into the contract enum.
func Direction(s string) (investapi.StopOrderDirection, error) {
	switch s {
	case "buy":
		return investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY, nil
	case "sell":
		return investapi.StopOrderDirection_STOP_ORDER_DIRECTION_SELL, nil
	default:
		return 0, fmt.Errorf("invalid direction %q: want buy or sell", s)
	}
}

// Type parses the CLI --type flag into the contract's stop-order-type enum.
func Type(s string) (investapi.StopOrderType, error) {
	switch s {
	case "take-profit":
		return investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT, nil
	case "stop-loss":
		return investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS, nil
	case "stop-limit":
		return investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT, nil
	default:
		return 0, fmt.Errorf("invalid stop order type %q: want take-profit, stop-loss, or stop-limit", s)
	}
}

// Expiration parses the CLI --expiration flag. An empty string defaults to
// good-till-cancel.
func Expiration(s string) (investapi.StopOrderExpirationType, error) {
	switch s {
	case "", "gtc":
		return investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL, nil
	case "gtd":
		return investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE, nil
	default:
		return 0, fmt.Errorf("invalid expiration %q: want gtc or gtd", s)
	}
}

// ExchangeOrderType parses the take-profit-only CLI --exchange-order-type
// flag. An empty string defaults to market.
func ExchangeOrderType(s string) (investapi.ExchangeOrderType, error) {
	switch s {
	case "", "market":
		return investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_MARKET, nil
	case "limit":
		return investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_LIMIT, nil
	default:
		return 0, fmt.Errorf("invalid exchange order type %q: want market or limit", s)
	}
}

// TakeProfitType parses the CLI --take-profit-type flag. An empty string
// defaults to regular (non-trailing).
func TakeProfitType(s string) (investapi.TakeProfitType, error) {
	switch s {
	case "", "regular":
		return investapi.TakeProfitType_TAKE_PROFIT_TYPE_REGULAR, nil
	case "trailing":
		return investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING, nil
	default:
		return 0, fmt.Errorf("invalid take-profit type %q: want regular or trailing", s)
	}
}

// TrailingValueType parses a --trailing-indent-type/--trailing-spread-type
// flag value.
func TrailingValueType(s string) (investapi.TrailingValueType, error) {
	switch s {
	case "absolute":
		return investapi.TrailingValueType_TRAILING_VALUE_ABSOLUTE, nil
	case "relative":
		return investapi.TrailingValueType_TRAILING_VALUE_RELATIVE, nil
	default:
		return 0, fmt.Errorf("invalid trailing value type %q: want absolute or relative", s)
	}
}

// Status parses the CLI --status flag for `stop-orders list`. An empty
// string means no filter (STOP_ORDER_STATUS_UNSPECIFIED — the broker's own
// default, which returns active orders).
func Status(s string) (investapi.StopOrderStatusOption, error) {
	switch s {
	case "":
		return investapi.StopOrderStatusOption_STOP_ORDER_STATUS_UNSPECIFIED, nil
	case "all":
		return investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ALL, nil
	case "active":
		return investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ACTIVE, nil
	case "executed":
		return investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXECUTED, nil
	case "canceled":
		return investapi.StopOrderStatusOption_STOP_ORDER_STATUS_CANCELED, nil
	case "expired":
		return investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXPIRED, nil
	default:
		return 0, fmt.Errorf("invalid status %q: want all, active, executed, canceled, or expired", s)
	}
}

// BasicsInput is the fully-parsed, not-yet-instrument-resolved shape of a
// stop-order placement, used by ValidateBasics. Trailing reuses TrailingParams
// (stoporders.go) rather than a second type, since PlaceParams needs the
// exact same shape.
type BasicsInput struct {
	StopOrderType     investapi.StopOrderType
	Quantity          int64
	Price             *investapi.Quotation // stop-limit only
	StopPrice         *investapi.Quotation // always required
	ExpirationType    investapi.StopOrderExpirationType
	HasExpireDate     bool
	ExchangeOrderType investapi.ExchangeOrderType
	TakeProfitType    investapi.TakeProfitType
	Trailing          *TrailingParams
}

// ValidateBasics runs the stop-order shape checks that need no instrument
// data (plan §9, mirroring internal/broker/orders.ValidateBasics): a positive
// quantity, a non-zero stop_price always, price present exactly when the
// order type is stop-limit, expire_date present exactly when expiration is
// GTD, and trailing fields present exactly when the order is a trailing
// take-profit. It runs before any network call so bad shapes fail with exit 2
// without a token.
func ValidateBasics(in BasicsInput) error {
	if in.Quantity <= 0 {
		return fmt.Errorf("quantity must be a positive number of lots, got %d", in.Quantity)
	}
	if isZeroQuotation(in.StopPrice) {
		return fmt.Errorf("stop-price is required and must be non-zero")
	}
	if in.StopPrice.GetUnits() < 0 || in.StopPrice.GetNano() < 0 {
		return fmt.Errorf("stop-price must not be negative")
	}

	switch in.StopOrderType {
	case investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LIMIT:
		if isZeroQuotation(in.Price) {
			return fmt.Errorf("stop-limit orders require a non-zero --price")
		}
		if in.Price.GetUnits() < 0 || in.Price.GetNano() < 0 {
			return fmt.Errorf("price must not be negative")
		}
	case investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT, investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS:
		if in.Price != nil {
			return fmt.Errorf("--price is only valid for stop-limit orders (%s does not use it)", typeName(in.StopOrderType))
		}
	default:
		return fmt.Errorf("--type is required: want take-profit, stop-loss, or stop-limit")
	}

	switch in.ExpirationType {
	case investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL:
		if in.HasExpireDate {
			return fmt.Errorf("--expire-date is only valid with --expiration gtd")
		}
	case investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE:
		if !in.HasExpireDate {
			return fmt.Errorf("--expiration gtd requires --expire-date")
		}
	default:
		return fmt.Errorf("--expiration is required: want gtc or gtd")
	}

	if in.Trailing != nil {
		if in.StopOrderType != investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT {
			return fmt.Errorf("trailing fields (--trailing-indent/--trailing-spread) are only valid for take-profit orders")
		}
		if isZeroQuotation(in.Trailing.Indent) || isZeroQuotation(in.Trailing.Spread) {
			return fmt.Errorf("trailing requires non-zero --trailing-indent and --trailing-spread")
		}
	}
	if in.StopOrderType != investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT &&
		in.TakeProfitType != investapi.TakeProfitType_TAKE_PROFIT_TYPE_UNSPECIFIED {
		return fmt.Errorf("--take-profit-type is only valid with --type take-profit")
	}
	if in.StopOrderType != investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT &&
		in.ExchangeOrderType != investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_UNSPECIFIED {
		return fmt.Errorf("--exchange-order-type is only valid with --type take-profit")
	}
	if in.TakeProfitType == investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING {
		if in.StopOrderType != investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT {
			return fmt.Errorf("--take-profit-type trailing is only valid with --type take-profit")
		}
		if in.Trailing == nil {
			return fmt.Errorf("--take-profit-type trailing requires --trailing-indent/--trailing-indent-type/--trailing-spread/--trailing-spread-type")
		}
	}

	return nil
}

func isZeroQuotation(q *investapi.Quotation) bool {
	return q == nil || (q.GetUnits() == 0 && q.GetNano() == 0)
}

func typeName(t investapi.StopOrderType) string {
	switch t {
	case investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT:
		return "take-profit"
	case investapi.StopOrderType_STOP_ORDER_TYPE_STOP_LOSS:
		return "stop-loss"
	default:
		return "stop-limit"
	}
}
