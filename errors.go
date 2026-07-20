package tinvest

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Dronnn/tinvest/internal/transport"
)

// APIError wraps every error a broker-facing Client method returns. It exposes
// the gRPC status code and, when the broker returned one, the x-tracking-id of
// the failing call — the identifier T-Bank support needs to investigate a
// request. (Close is not broker-facing: it returns the underlying gRPC
// connection-close error unwrapped.)
//
// GRPCCode is the gRPC status code of a broker failure; for an error detected
// locally before any call was made (a malformed identifier, an invalid
// order-book depth, an unknown instrument type or candle interval) it is
// codes.Unknown and TrackingID is empty. Unwrap returns the underlying error,
// so errors.Is / errors.As against gRPC status errors and the internal typed
// errors keep working through the wrapper.
type APIError struct {
	// GRPCCode is the gRPC status code of the failure (codes.Unknown for an
	// error detected before the request reached the broker).
	GRPCCode codes.Code
	// TrackingID is the broker's x-tracking-id for the failing call, or "" when
	// the broker returned none (including local, pre-flight failures).
	TrackingID string

	err error
}

func (e *APIError) Error() string {
	if e.TrackingID != "" {
		return fmt.Sprintf("tinvest: %v (grpc code %s, tracking id %s)", e.err, e.GRPCCode, e.TrackingID)
	}
	return fmt.Sprintf("tinvest: %v (grpc code %s)", e.err, e.GRPCCode)
}

func (e *APIError) Unwrap() error { return e.err }

// apiErr wraps a method error as *APIError, attaching the gRPC status code and
// the tracking id captured for the call (both empty/Unknown for a local
// failure). It returns nil for a nil error, so a call site can write
// `return result, apiErr(err, info)` uniformly. info may be nil for an error
// raised before any call context existed.
func apiErr(err error, info *transport.CallInfo) error {
	if err == nil {
		return nil
	}
	trackingID := ""
	if info != nil {
		trackingID = info.TrackingID()
	}
	return &APIError{GRPCCode: status.Code(err), TrackingID: trackingID, err: err}
}
