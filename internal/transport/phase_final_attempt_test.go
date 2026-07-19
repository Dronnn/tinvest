package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/transport/retry"
)

// TestPhaseReflectsFinalAttemptNotHighWaterMark is the F1 regression: across
// retry attempts sharing one CallInfo, a first attempt that reached "confirmed"
// (a retryable server status with a trailer) must not mask a final attempt that
// went on the wire and died mid-send. The final attempt ended sent_unconfirmed,
// so the call must report sent_unconfirmed (which classifies a mutation as
// UNCONFIRMED / exit 7, not a plain network error). This fails on eabce2e, where
// CallInfo advanced monotonically and reported confirmed.
func TestPhaseReflectsFinalAttemptNotHighWaterMark(t *testing.T) {
	var calls int32
	reached2 := make(chan struct{}, 1)
	fake := &fakeUsers{handler: func(ctx context.Context) (*investapi.GetAccountsResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			_ = grpc.SetTrailer(ctx, metadata.Pairs("x-tracking-id", "trk-attempt1"))
			return nil, status.Error(codes.Unavailable, "retry me") // confirmed + retryable
		}
		// Retry attempt: the request reaches the server (so it went on the wire),
		// then hangs until the shared deadline fires -> DeadlineExceeded, no
		// server trailer -> the attempt ends sent_unconfirmed.
		select {
		case reached2 <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	lis := startServer(t, fake)
	policy := retry.RetryPolicy{MaxAttempts: 3, PerCallCodes: []codes.Code{codes.Unavailable}}
	conn := dialBuf(t, lis, Config{Token: "t", Timeout: 700 * time.Millisecond, RetryPolicy: &policy})

	ctx, info := WithCallInfo(retry.Idempotent(context.Background()))
	_, err := investapi.NewUsersServiceClient(conn).GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded from the final attempt", err)
	}
	select {
	case <-reached2:
	case <-time.After(2 * time.Second):
		t.Fatal("retry attempt never reached the server")
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2 (one retry after UNAVAILABLE)", got)
	}
	if info.Phase() != PhaseSentUnconfirmed {
		t.Errorf("phase = %s, want sent_unconfirmed (final attempt died mid-send, must not be masked by attempt 1's confirmation)", info.Phase())
	}
}

// TestCallInfoPhaseComposition locks the exact per-attempt semantics of the F1
// fix directly against the phase-tracking primitives: `confirmed` is per-attempt
// (reset by beginAttempt) so the final attempt wins, while `sent` is a monotonic
// high-water mark so an earlier attempt that reached the wire is never forgotten
// by a later attempt that never sent.
func TestCallInfoPhaseComposition(t *testing.T) {
	t.Run("confirmed then final sent-only -> sent_unconfirmed", func(t *testing.T) {
		c := &CallInfo{}
		c.beginAttempt()
		c.markSent()
		c.markConfirmed() // attempt 1 confirmed
		c.beginAttempt()
		c.markSent() // attempt 2 on the wire, no confirmation
		if got := c.Phase(); got != PhaseSentUnconfirmed {
			t.Errorf("phase = %s, want sent_unconfirmed", got)
		}
	})
	t.Run("sent then final never-sent -> sent_unconfirmed (high-water mark)", func(t *testing.T) {
		c := &CallInfo{}
		c.beginAttempt()
		c.markSent() // attempt 1 went on the wire, died
		c.beginAttempt()
		// attempt 2 never reached the wire
		if got := c.Phase(); got != PhaseSentUnconfirmed {
			t.Errorf("phase = %s, want sent_unconfirmed (earlier send must survive)", got)
		}
	})
	t.Run("final attempt confirmed -> confirmed", func(t *testing.T) {
		c := &CallInfo{}
		c.beginAttempt()
		c.markSent()
		c.beginAttempt()
		c.markSent()
		c.markConfirmed()
		if got := c.Phase(); got != PhaseConfirmed {
			t.Errorf("phase = %s, want confirmed", got)
		}
	})
	t.Run("never sent -> not_sent", func(t *testing.T) {
		c := &CallInfo{}
		c.beginAttempt()
		if got := c.Phase(); got != PhaseNotSent {
			t.Errorf("phase = %s, want not_sent", got)
		}
	})
}
