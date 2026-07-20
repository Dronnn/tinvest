package stoporders

import investapi "github.com/Dronnn/tinvest/pb/investapi"

// Status is the CLI's stable, machine-facing name for a stop order's status
// (mirrors internal/broker/orders.Lifecycle's role for regular orders): the
// contract's StopOrderStatusOption enum is the source; these names are what
// appears in JSON so consumers never depend on the proto enum spelling.
const (
	StatusUnspecified = "unspecified"
	StatusActive      = "active"
	StatusExecuted    = "executed"
	StatusCanceled    = "canceled"
	StatusExpired     = "expired"
)

// StatusName maps a contract stop-order status to its stable CLI name.
func StatusName(s investapi.StopOrderStatusOption) string {
	switch s {
	case investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ACTIVE:
		return StatusActive
	case investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXECUTED:
		return StatusExecuted
	case investapi.StopOrderStatusOption_STOP_ORDER_STATUS_CANCELED:
		return StatusCanceled
	case investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXPIRED:
		return StatusExpired
	default:
		return StatusUnspecified
	}
}

// IsTerminalStatus reports whether a stop-order status is final: executed,
// cancelled, or expired. A stop order in a terminal status will not change, so a
// cancel that finds it already terminal is a satisfied no-op (finding F5).
func IsTerminalStatus(s investapi.StopOrderStatusOption) bool {
	switch s {
	case investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXECUTED,
		investapi.StopOrderStatusOption_STOP_ORDER_STATUS_CANCELED,
		investapi.StopOrderStatusOption_STOP_ORDER_STATUS_EXPIRED:
		return true
	default:
		return false
	}
}
