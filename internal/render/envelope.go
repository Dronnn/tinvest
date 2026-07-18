package render

import (
	"encoding/json"
	"io"
	"time"
)

// SchemaVersion versions the CLI's own JSON output contract, independently of
// the protobuf contract.
const SchemaVersion = "0.1"

// Contract is the pinned protobuf contract release. Keep in sync with
// proto/VERSION.md whenever the vendored contracts are updated.
const Contract = "1.49"

// Meta is the envelope metadata attached to every response.
type Meta struct {
	AccountID     string `json:"account_id,omitempty"`
	TrackingID    string `json:"tracking_id,omitempty"`
	ElapsedMS     int64  `json:"elapsed_ms"`
	Contract      string `json:"contract"`
	SchemaVersion string `json:"schema_version"`
}

// NewMeta builds envelope metadata; contract and schema version are filled in.
func NewMeta(accountID, trackingID string, elapsed time.Duration) Meta {
	return Meta{
		AccountID:     accountID,
		TrackingID:    trackingID,
		ElapsedMS:     elapsed.Milliseconds(),
		Contract:      Contract,
		SchemaVersion: SchemaVersion,
	}
}

// Envelope is the uniform JSON wrapper for every command result (plan §7).
type Envelope struct {
	OK    bool       `json:"ok"`
	Data  any        `json:"data,omitempty"`
	Error *ErrorBody `json:"error,omitempty"`
	Meta  Meta       `json:"meta"`
}

// ErrorBody is the machine-readable error block of a failure envelope.
type ErrorBody struct {
	Code         Code   `json:"code"`
	GRPCCode     string `json:"grpc_code,omitempty"`
	APICode      string `json:"api_code,omitempty"`
	Message      string `json:"message"`
	Retryable    bool   `json:"retryable"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
	Phase        string `json:"phase,omitempty"`
	TrackingID   string `json:"tracking_id,omitempty"`
}

// Success wraps data in an ok envelope.
func Success(data any, meta Meta) Envelope {
	return Envelope{OK: true, Data: data, Meta: meta}
}

// Failure wraps a classified error in a failure envelope.
func Failure(err *CLIError, meta Meta) Envelope {
	return Envelope{OK: false, Error: err.Body(), Meta: meta}
}

// WriteJSON writes the envelope as indented JSON followed by a newline.
func WriteJSON(w io.Writer, env Envelope) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(env)
}
