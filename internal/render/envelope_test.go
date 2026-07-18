package render

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"tinvest/internal/transport"
)

var update = flag.Bool("update", false, "rewrite golden files")

func checkGolden(t *testing.T, name string, env Envelope) {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteJSON(&buf, env); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatalf("update golden: %v", err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("envelope mismatch for %s:\ngot:\n%s\nwant:\n%s", name, buf.Bytes(), want)
	}
}

func fixedMeta() Meta {
	return NewMeta("2001234567", "trk-0f9a", 123*time.Millisecond)
}

func TestEnvelopeGoldenSuccess(t *testing.T) {
	data := struct {
		Accounts []AccountView `json:"accounts"`
	}{
		Accounts: []AccountView{{
			ID:          "2001234567",
			Name:        "Main",
			Type:        "ACCOUNT_TYPE_TINKOFF",
			Status:      "ACCOUNT_STATUS_OPEN",
			OpenedDate:  "2020-01-02T00:00:00Z",
			AccessLevel: "ACCOUNT_ACCESS_LEVEL_FULL_ACCESS",
		}},
	}
	checkGolden(t, "success.json", Success(data, fixedMeta()))
}

func TestEnvelopeGoldenErrors(t *testing.T) {
	cases := []struct {
		golden string
		err    error
		cc     CallContext
	}{
		{
			golden: "error_auth.json",
			err:    status.Error(codes.Unauthenticated, "40003"),
			cc: CallContext{
				Phase:      transport.PhaseConfirmed,
				TrackingID: "trk-0f9a",
				APIMessage: "Token is invalid or expired",
			},
		},
		{
			golden: "error_rate_limited.json",
			err:    status.Error(codes.ResourceExhausted, "80002"),
			cc: CallContext{
				Phase:      transport.PhaseConfirmed,
				TrackingID: "trk-0f9a",
				RetryAfter: 5 * time.Second,
				APIMessage: "Request limit exceeded",
			},
		},
		{
			golden: "error_broker_rejected.json",
			err:    status.Error(codes.InvalidArgument, "30001"),
			cc: CallContext{
				Phase:      transport.PhaseConfirmed,
				TrackingID: "trk-0f9a",
				APIMessage: "Invalid parameter value",
			},
		},
		{
			golden: "error_network.json",
			err:    status.Error(codes.Unavailable, "connection refused"),
			cc:     CallContext{Phase: transport.PhaseNotSent},
		},
		{
			golden: "error_unconfirmed.json",
			err:    status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			cc:     CallContext{Phase: transport.PhaseSentUnconfirmed, Mutation: true},
		},
		{
			golden: "error_internal.json",
			err:    status.Error(codes.Internal, "70001"),
			cc: CallContext{
				Phase:      transport.PhaseConfirmed,
				TrackingID: "trk-0f9a",
				APIMessage: "Internal error, try again later",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.golden, func(t *testing.T) {
			checkGolden(t, tc.golden, Failure(Classify(tc.err, tc.cc), fixedMeta()))
		})
	}
}

func TestEnvelopeGoldenUsage(t *testing.T) {
	env := Failure(UsageError(`invalid output format "yaml" (want json or table)`), NewMeta("", "", 0))
	checkGolden(t, "error_usage.json", env)
}

func TestEnvelopeGoldenAuthNoToken(t *testing.T) {
	env := Failure(AuthError("no token configured: set TINVEST_TOKEN, use --token-file, or configure token_file in a profile"), NewMeta("", "", 0))
	checkGolden(t, "error_auth_no_token.json", env)
}
