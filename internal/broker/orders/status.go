package orders

import investapi "github.com/Dronnn/tinvest/pb/investapi"

// Lifecycle is the CLI's stable, machine-facing name for an order's execution
// state (plan §9). The contract's OrderExecutionReportStatus enum is the
// source; these names are what appears in JSON so consumers never depend on the
// proto enum spelling. The contract enum (contract 1.49) carries only the five
// non-unspecified states below — "accepted"/"cancelling"/"expired" from the
// prose lifecycle have no distinct enum value here, so they are not emitted;
// requested vs executed vs remaining lots are reported separately by the view.
const (
	LifecycleUnspecified     = "unspecified"
	LifecycleNew             = "new"
	LifecyclePartiallyFilled = "partially-filled"
	LifecycleFilled          = "filled"
	LifecycleCancelled       = "cancelled"
	LifecycleRejected        = "rejected"
)

// Lifecycle maps a contract execution-report status to its stable CLI name.
func Lifecycle(s investapi.OrderExecutionReportStatus) string {
	switch s {
	case investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_NEW:
		return LifecycleNew
	case investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_PARTIALLYFILL:
		return LifecyclePartiallyFilled
	case investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL:
		return LifecycleFilled
	case investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED:
		return LifecycleCancelled
	case investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_REJECTED:
		return LifecycleRejected
	default:
		return LifecycleUnspecified
	}
}

// IsTerminal reports whether a status is final: filled, cancelled, or rejected.
// A terminal order will not change state, so `orders wait` stops polling once it
// sees one.
func IsTerminal(s investapi.OrderExecutionReportStatus) bool {
	switch s {
	case investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL,
		investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_CANCELLED,
		investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_REJECTED:
		return true
	default:
		return false
	}
}

// IsRejected reports whether a placement was definitively rejected by the
// broker (business error → exit 5, plan §7).
func IsRejected(s investapi.OrderExecutionReportStatus) bool {
	return s == investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_REJECTED
}
