package transport

import (
	"context"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

// Phase tracks how far a call got, so the render layer can distinguish
// "nothing reached the broker" from "sent, outcome unknown" (plan §7/§9).
type Phase int

const (
	// PhaseNotSent: the request never went on the wire.
	PhaseNotSent Phase = iota
	// PhaseSentUnconfirmed: the request was written to the wire but no
	// definitive answer arrived from the server (e.g. deadline after send).
	PhaseSentUnconfirmed
	// PhaseConfirmed: a response or a definitive gRPC status arrived from the
	// server — including server-side errors.
	PhaseConfirmed
)

func (p Phase) String() string {
	switch p {
	case PhaseSentUnconfirmed:
		return "sent_unconfirmed"
	case PhaseConfirmed:
		return "confirmed"
	default:
		return "not_sent"
	}
}

// CallInfo collects per-call observations made by the transport layer:
// call phase, x-tracking-id, rate-limit reset, and the human-readable error
// description the broker puts in the "message" trailer.
//
// Phase is derived from two facts rather than a single monotonic value so that,
// across retry attempts sharing this CallInfo, the classification reflects the
// FINAL attempt's outcome (plan §7/§9, finding F1): `sent` is a high-water mark
// (any attempt that reached the wire may have reached the broker), while
// `confirmed` is reset at the start of each attempt (beginAttempt) and set only
// if THAT attempt received a definitive server response. Without the reset, an
// earlier attempt that confirmed would mask a final attempt that ended
// sent_unconfirmed, misclassifying an unconfirmable mutation as exit 6 instead
// of exit 7.
type CallInfo struct {
	mu         sync.Mutex
	sent       bool // any attempt put the request on the wire (monotonic)
	confirmed  bool // the LAST attempt received a definitive server response
	trackingID string
	apiMessage string
	retryAfter time.Duration
}

// Phase returns the phase the call ended in, derived from the final attempt's
// confirmation and whether any attempt reached the wire.
func (c *CallInfo) Phase() Phase {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.confirmed:
		return PhaseConfirmed
	case c.sent:
		return PhaseSentUnconfirmed
	default:
		return PhaseNotSent
	}
}

// TrackingID returns the x-tracking-id sent by the broker, if any.
func (c *CallInfo) TrackingID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trackingID
}

// APIMessage returns the broker's human-readable error description, if any.
func (c *CallInfo) APIMessage() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.apiMessage
}

// RetryAfter returns the rate-limit reset interval reported by the broker,
// or zero.
func (c *CallInfo) RetryAfter() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.retryAfter
}

// beginAttempt resets the per-attempt confirmation at the start of each RPC
// attempt (stats.Begin) so the final classification reflects the LAST attempt,
// not a high-water mark. `sent` is deliberately NOT reset: if any attempt
// reached the wire the mutation may have reached the broker, so that fact must
// survive a later attempt that never sent.
func (c *CallInfo) beginAttempt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirmed = false
}

func (c *CallInfo) markSent() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = true
}

func (c *CallInfo) markConfirmed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confirmed = true
}

func (c *CallInfo) captureMD(md metadata.MD) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v := md.Get("x-tracking-id"); len(v) > 0 {
		c.trackingID = v[0]
	}
	if v := md.Get("message"); len(v) > 0 {
		c.apiMessage = v[0]
	}
	if v := md.Get("x-ratelimit-reset"); len(v) > 0 {
		if secs, err := strconv.Atoi(v[0]); err == nil && secs >= 0 {
			c.retryAfter = time.Duration(secs) * time.Second
		}
	}
}

type callInfoKey struct{}

// WithCallInfo derives a context carrying a fresh CallInfo for exactly one
// call. The transport's stats handler fills it in as the call progresses.
func WithCallInfo(ctx context.Context) (context.Context, *CallInfo) {
	info := &CallInfo{}
	return context.WithValue(ctx, callInfoKey{}, info), info
}

func callInfoFrom(ctx context.Context) *CallInfo {
	info, _ := ctx.Value(callInfoKey{}).(*CallInfo)
	return info
}

// phaseStats drives phase transitions off the actual wire events: OutPayload
// is the only reliable "went on the wire" signal, and a server-sent trailer
// (which carries the definitive status, success or error) is the only
// confirmation. Client-generated errors such as DEADLINE_EXCEEDED after send
// produce no trailer and therefore leave the call sent_unconfirmed.
type phaseStats struct{}

func (phaseStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

func (phaseStats) HandleRPC(ctx context.Context, s stats.RPCStats) {
	info := callInfoFrom(ctx)
	if info == nil {
		return
	}
	switch s := s.(type) {
	case *stats.Begin:
		info.beginAttempt()
	case *stats.OutPayload:
		info.markSent()
	case *stats.InHeader:
		info.captureMD(s.Header)
	case *stats.InTrailer:
		info.captureMD(s.Trailer)
		info.markConfirmed()
	case *stats.End:
		if s.Error == nil {
			info.markConfirmed()
		}
	}
}

func (phaseStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (phaseStats) HandleConn(context.Context, stats.ConnStats)                       {}
