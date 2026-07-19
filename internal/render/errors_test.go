package render

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"tinvest/internal/transport"
)

func TestClassifyExitCodes(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		cc       CallContext
		wantCode Code
		wantExit int
	}{
		{
			name:     "unauthenticated",
			err:      status.Error(codes.Unauthenticated, "40003"),
			cc:       CallContext{Phase: transport.PhaseConfirmed},
			wantCode: CodeAuth,
			wantExit: 3,
		},
		{
			name:     "permission denied",
			err:      status.Error(codes.PermissionDenied, "40002"),
			cc:       CallContext{Phase: transport.PhaseConfirmed},
			wantCode: CodeAuth,
			wantExit: 3,
		},
		{
			name:     "api code 40003 wins over transport code",
			err:      status.Error(codes.Unknown, "40003"),
			cc:       CallContext{Phase: transport.PhaseConfirmed},
			wantCode: CodeAuth,
			wantExit: 3,
		},
		{
			name:     "rate limited",
			err:      status.Error(codes.ResourceExhausted, "80002"),
			cc:       CallContext{Phase: transport.PhaseConfirmed, RetryAfter: 5 * time.Second},
			wantCode: CodeRateLimited,
			wantExit: 4,
		},
		{
			name:     "broker rejection invalid argument",
			err:      status.Error(codes.InvalidArgument, "30001"),
			cc:       CallContext{Phase: transport.PhaseConfirmed},
			wantCode: CodeBrokerRejected,
			wantExit: 5,
		},
		{
			name:     "broker rejection failed precondition",
			err:      status.Error(codes.FailedPrecondition, "30079"),
			cc:       CallContext{Phase: transport.PhaseConfirmed},
			wantCode: CodeBrokerRejected,
			wantExit: 5,
		},
		{
			name:     "broker rejection not found",
			err:      status.Error(codes.NotFound, "50005"),
			cc:       CallContext{Phase: transport.PhaseConfirmed},
			wantCode: CodeBrokerRejected,
			wantExit: 5,
		},
		{
			name:     "network before send",
			err:      status.Error(codes.Unavailable, "connection refused"),
			cc:       CallContext{Phase: transport.PhaseNotSent},
			wantCode: CodeNetwork,
			wantExit: 6,
		},
		{
			name:     "read timed out after send maps to network",
			err:      status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			cc:       CallContext{Phase: transport.PhaseSentUnconfirmed},
			wantCode: CodeNetwork,
			wantExit: 6,
		},
		{
			name:     "mutation timed out after send is unconfirmed",
			err:      status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			cc:       CallContext{Phase: transport.PhaseSentUnconfirmed, Mutation: true},
			wantCode: CodeUnconfirmed,
			wantExit: 7,
		},
		{
			name:     "mutation connection drop after send is unconfirmed",
			err:      status.Error(codes.Unavailable, "connection reset"),
			cc:       CallContext{Phase: transport.PhaseSentUnconfirmed, Mutation: true},
			wantCode: CodeUnconfirmed,
			wantExit: 7,
		},
		{
			name:     "mutation not sent maps to network",
			err:      status.Error(codes.Unavailable, "connection refused"),
			cc:       CallContext{Phase: transport.PhaseNotSent, Mutation: true},
			wantCode: CodeNetwork,
			wantExit: 6,
		},
		{
			name:     "server internal",
			err:      status.Error(codes.Internal, "70001"),
			cc:       CallContext{Phase: transport.PhaseConfirmed},
			wantCode: CodeInternal,
			wantExit: 1,
		},
		{
			name:     "non-status error",
			err:      errors.New("boom"),
			cc:       CallContext{Phase: transport.PhaseNotSent},
			wantCode: CodeInternal,
			wantExit: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.err, tc.cc)
			if got.Code != tc.wantCode {
				t.Errorf("code = %s, want %s", got.Code, tc.wantCode)
			}
			if got.ExitCode() != tc.wantExit {
				t.Errorf("exit = %d, want %d", got.ExitCode(), tc.wantExit)
			}
		})
	}
}

// TestClassify30057DuplicateReportNotFoundDemandsReconcile: API code 30057
// ("duplicate order, but the order report was not found") fires in the
// idempotent-retry path when the broker recognizes our order_id but cannot
// return the original report — the order may exist. On a mutation it must
// classify UNCONFIRMED (exit 7), never a plain rejection that would invite a
// duplicate. It arrives like other broker codes: the numeric code as the status
// message, the human text in the "message" trailer (APIMessage).
func TestClassify30057DuplicateReportNotFoundDemandsReconcile(t *testing.T) {
	err := status.Error(codes.InvalidArgument, "30057")

	mut := Classify(err, CallContext{
		Phase:      transport.PhaseConfirmed,
		Mutation:   true,
		APIMessage: "The order is a duplicate, but the order report was not found",
	})
	if mut.Code != CodeUnconfirmed {
		t.Errorf("mutation 30057 code = %s, want UNCONFIRMED", mut.Code)
	}
	if mut.ExitCode() != ExitUnconfirmed {
		t.Errorf("mutation 30057 exit = %d, want %d", mut.ExitCode(), ExitUnconfirmed)
	}
	if mut.APICode != "30057" {
		t.Errorf("api code = %q, want 30057", mut.APICode)
	}

	// A non-mutation call is not subject to the duplicate-order case.
	read := Classify(err, CallContext{Phase: transport.PhaseConfirmed})
	if read.Code != CodeBrokerRejected {
		t.Errorf("non-mutation 30057 code = %s, want BROKER_REJECTED", read.Code)
	}
}

func TestClassifyDetails(t *testing.T) {
	err := status.Error(codes.ResourceExhausted, "80002")
	got := Classify(err, CallContext{
		Phase:      transport.PhaseConfirmed,
		TrackingID: "trk-1",
		RetryAfter: 5 * time.Second,
		APIMessage: "Request limit exceeded",
	})
	if !got.Retryable {
		t.Error("rate-limited must be retryable")
	}
	if got.RetryAfter != 5*time.Second {
		t.Errorf("retry after = %v, want 5s", got.RetryAfter)
	}
	if got.APICode != "80002" {
		t.Errorf("api code = %q, want 80002", got.APICode)
	}
	if got.Message != "Request limit exceeded" {
		t.Errorf("message = %q, want trailer description", got.Message)
	}
	if got.TrackingID != "trk-1" {
		t.Errorf("tracking id = %q", got.TrackingID)
	}
	if got.GRPCCode != "RESOURCE_EXHAUSTED" {
		t.Errorf("grpc code = %q", got.GRPCCode)
	}
}

func TestUsageAndAuthErrors(t *testing.T) {
	if got := UsageError("bad flag").ExitCode(); got != 2 {
		t.Errorf("usage exit = %d, want 2", got)
	}
	if got := AuthError("no token").ExitCode(); got != 3 {
		t.Errorf("auth exit = %d, want 3", got)
	}
}
