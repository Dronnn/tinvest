package render

import (
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"tinvest/internal/transport"
)

// Code is the stable CLI-level error classification (plan §7).
type Code string

const (
	CodeUsage          Code = "USAGE"
	CodePolicy         Code = "POLICY"
	CodeAuth           Code = "AUTH"
	CodeRateLimited    Code = "RATE_LIMITED"
	CodeBrokerRejected Code = "BROKER_REJECTED"
	CodeNetwork        Code = "NETWORK"
	CodeUnconfirmed    Code = "UNCONFIRMED"
	CodeInternal       Code = "INTERNAL"
)

// Exit codes are a stable contract (plan §7).
const (
	ExitOK          = 0
	ExitInternal    = 1
	ExitUsage       = 2
	ExitAuth        = 3
	ExitRateLimited = 4
	ExitRejected    = 5
	ExitNetwork     = 6
	ExitUnconfirmed = 7
)

// CLIError is a classified command failure carrying everything the error
// envelope and the exit code need.
type CLIError struct {
	Code       Code
	GRPCCode   string
	APICode    string
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	Phase      string
	TrackingID string
	// Details carries machine-readable key/value context, used by policy
	// violations (plan §6) to name the breached rule and its bound and actual
	// values. Nil for most errors.
	Details map[string]string
	// ReconcileHint is the machine-readable recovery instruction attached to an
	// UNCONFIRMED (exit 7) mutation: the order_id to reconcile plus the command
	// that resolves it (plan §9).
	ReconcileHint *ReconcileHint
}

// ReconcileHint tells an agent how to converge on the true outcome of a
// mutation whose result is unknown (plan §9).
type ReconcileHint struct {
	OrderID string `json:"order_id,omitempty"`
	Command string `json:"command"`
}

func (e *CLIError) Error() string { return e.Message }

// ExitCode maps the classification to the process exit code.
func (e *CLIError) ExitCode() int {
	switch e.Code {
	case CodeUsage, CodePolicy:
		return ExitUsage
	case CodeAuth:
		return ExitAuth
	case CodeRateLimited:
		return ExitRateLimited
	case CodeBrokerRejected:
		return ExitRejected
	case CodeNetwork:
		return ExitNetwork
	case CodeUnconfirmed:
		return ExitUnconfirmed
	default:
		return ExitInternal
	}
}

// Body converts the error to its envelope representation.
func (e *CLIError) Body() *ErrorBody {
	return &ErrorBody{
		Code:          e.Code,
		GRPCCode:      e.GRPCCode,
		APICode:       e.APICode,
		Message:       e.Message,
		Retryable:     e.Retryable,
		RetryAfterMS:  e.RetryAfter.Milliseconds(),
		Phase:         e.Phase,
		TrackingID:    e.TrackingID,
		Details:       e.Details,
		ReconcileHint: e.ReconcileHint,
	}
}

// UsageError builds a validation/usage failure (nothing sent to the broker).
func UsageError(message string) *CLIError {
	return &CLIError{Code: CodeUsage, Message: message}
}

// PolicyError builds a policy-guardrail violation (plan §6): exit 2, code
// POLICY, with machine-readable details naming the breached rule. Nothing was
// sent to the broker and no ledger entry was created.
func PolicyError(message string, details map[string]string) *CLIError {
	return &CLIError{Code: CodePolicy, Message: message, Details: details}
}

// UnconfirmedError builds an exit-7 unknown-state failure for a mutation that
// went on the wire but whose outcome could not be confirmed (plan §9). The
// order_id and reconcile command are surfaced so a caller can converge.
func UnconfirmedError(message, orderID, reconcileCmd string, cc CallContext) *CLIError {
	return &CLIError{
		Code:          CodeUnconfirmed,
		Message:       message,
		Phase:         cc.Phase.String(),
		TrackingID:    cc.TrackingID,
		ReconcileHint: &ReconcileHint{OrderID: orderID, Command: reconcileCmd},
	}
}

// AuthError builds an auth failure that did not come from a gRPC call
// (e.g. no token configured).
func AuthError(message string) *CLIError {
	return &CLIError{Code: CodeAuth, Message: message}
}

// CallContext is what the caller observed about the failed call, taken from
// transport.CallInfo plus whether the call was a mutation.
type CallContext struct {
	Phase      transport.Phase
	TrackingID string
	RetryAfter time.Duration
	APIMessage string
	Mutation   bool
}

// Classify maps a gRPC call error to the stable CLI classification.
//
// The phase rules (plan §7): a mutation that ended sent_unconfirmed is
// UNCONFIRMED (exit 7) no matter the local status code — the outcome is
// unknown and must be reconciled. Reads that ended sent_unconfirmed are plain
// network failures (exit 6): nothing to reconcile, safe to retry. Server-sent
// statuses always arrive with phase confirmed, so they are classified by code.
func Classify(err error, cc CallContext) *CLIError {
	st, ok := status.FromError(err)
	if !ok {
		return &CLIError{
			Code:       CodeInternal,
			Message:    err.Error(),
			Phase:      cc.Phase.String(),
			TrackingID: cc.TrackingID,
		}
	}

	e := &CLIError{
		GRPCCode:   grpcCodeName(st.Code()),
		APICode:    apiCode(st.Message()),
		Message:    st.Message(),
		Phase:      cc.Phase.String(),
		TrackingID: cc.TrackingID,
	}
	if cc.APIMessage != "" {
		// The broker sends the numeric code as the status message and the
		// human-readable description in the "message" trailer.
		e.Message = cc.APIMessage
	}

	if cc.Mutation && cc.Phase == transport.PhaseSentUnconfirmed {
		e.Code = CodeUnconfirmed
		return e
	}
	// 30057: "duplicate order, but the order report was not found." The broker
	// recognized our order_id (an idempotent retry) but cannot return the placed
	// order's report, so the original order may well exist. On a mutation this
	// must NOT be a plain rejection — that would tempt a caller to place a
	// replacement and duplicate. Classify it UNCONFIRMED (exit 7) so the caller
	// reconciles against the broker before any retry (plan §9).
	if cc.Mutation && e.APICode == "30057" {
		e.Code = CodeUnconfirmed
		return e
	}
	if e.APICode == "40003" { // invalid token, regardless of transport code
		e.Code = CodeAuth
		return e
	}

	switch st.Code() {
	case codes.Unauthenticated, codes.PermissionDenied:
		e.Code = CodeAuth
	case codes.ResourceExhausted:
		e.Code = CodeRateLimited
		e.Retryable = true
		e.RetryAfter = cc.RetryAfter
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound:
		e.Code = CodeBrokerRejected
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		e.Code = CodeNetwork
		e.Retryable = true
	default:
		e.Code = CodeInternal
	}
	return e
}

// grpcCodeName renders a status code under its canonical gRPC name
// (RESOURCE_EXHAUSTED, not Go's ResourceExhausted).
func grpcCodeName(c codes.Code) string {
	if name, ok := grpcCodeNames[c]; ok {
		return name
	}
	return c.String()
}

var grpcCodeNames = map[codes.Code]string{
	codes.OK:                 "OK",
	codes.Canceled:           "CANCELLED",
	codes.Unknown:            "UNKNOWN",
	codes.InvalidArgument:    "INVALID_ARGUMENT",
	codes.DeadlineExceeded:   "DEADLINE_EXCEEDED",
	codes.NotFound:           "NOT_FOUND",
	codes.AlreadyExists:      "ALREADY_EXISTS",
	codes.PermissionDenied:   "PERMISSION_DENIED",
	codes.ResourceExhausted:  "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION",
	codes.Aborted:            "ABORTED",
	codes.OutOfRange:         "OUT_OF_RANGE",
	codes.Unimplemented:      "UNIMPLEMENTED",
	codes.Internal:           "INTERNAL",
	codes.Unavailable:        "UNAVAILABLE",
	codes.DataLoss:           "DATA_LOSS",
	codes.Unauthenticated:    "UNAUTHENTICATED",
}

// apiCode extracts the broker's own numeric error code, which the API places
// in the gRPC status message.
func apiCode(message string) string {
	if message == "" {
		return ""
	}
	for _, r := range message {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return message
}
