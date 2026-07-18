// Package stream owns resilient stream connection lifecycle: reconnect,
// authoritative subscription replay, ping/data watchdog, reconciliation, and
// explicit consumer-visible gap events.
package stream

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const DefaultWatchdog = 30 * time.Second

const DefaultReconcileBuffer = 10_000

type EventType string

const (
	EventConnected    EventType = "connected"
	EventDisconnected EventType = "disconnected"
	EventResubscribed EventType = "resubscribed"
	EventLagging      EventType = "lagging"
)

// LifecycleEvent reports stream state transitions without hiding data gaps.
type LifecycleEvent struct {
	Type          EventType
	Time          time.Time
	Attempt       int
	Subscriptions int
	Reason        string
	Err           error
	Final         bool
}

// Session adapts generated bidi and server-stream clients to one runner.
// Send is nil for a server-side stream because its initial request is replayed
// by Open whenever a new stream is established.
type Session[Request, Response any] struct {
	Recv      func() (*Response, error)
	Send      func(*Request) error
	CloseSend func() error
}

type OpenFunc[Request, Response any] func(context.Context) (Session[Request, Response], error)

// Runner reconnects until its context is canceled or a consumer callback
// fails. Cancellation is a clean shutdown: a final disconnected event is
// emitted and Run returns nil.
type Runner[Request, Response any] struct {
	Open                  OpenFunc[Request, Response]
	Subscriptions         *Registry[Request]
	Watchdog              time.Duration
	Backoff               func(attempt int) time.Duration
	ReplayLimit           int
	ReplayWindow          time.Duration
	MaxReconcileBuffer    int
	OnLifecycle           func(LifecycleEvent) error
	OnMessage             func(*Response) error
	IsActivity            func(*Response) bool
	Reconcile             func(context.Context) error
	BufferDuringReconcile func(*Response) bool
	// KeepAfterReconcile filters messages buffered while reconciliation was
	// in progress. KeepLiveAfterReconcile optionally filters later queued/live
	// messages until a protocol-specific ordering boundary is observed.
	KeepAfterReconcile     func(*Response) bool
	KeepLiveAfterReconcile func(*Response) bool
}

type receiveResult[T any] struct {
	message *T
	err     error
}

func (r Runner[Request, Response]) Run(ctx context.Context) error {
	if r.Open == nil {
		return errors.New("stream open function is required")
	}
	watchdog := r.Watchdog
	if watchdog <= 0 {
		watchdog = DefaultWatchdog
	}
	backoff := r.Backoff
	if backoff == nil {
		backoff = defaultBackoff
	}
	replayLimiter := newReplayLimiter(r.ReplayLimit, r.ReplayWindow)
	maxReconcileBuffer := r.MaxReconcileBuffer
	if maxReconcileBuffer <= 0 {
		maxReconcileBuffer = DefaultReconcileBuffer
	}

	connections := 0
	failures := 0
	for {
		if ctx.Err() != nil {
			return r.shutdown(connections)
		}
		replayRequests, subscriptionCount := r.Subscriptions.ReplaySnapshot()
		streamCtx, cancel := context.WithCancel(ctx)
		session, err := r.Open(streamCtx)
		if err != nil {
			cancel()
			failures++
			if emitErr := r.emit(LifecycleEvent{
				Type: EventDisconnected, Time: time.Now().UTC(), Attempt: failures,
				Reason: "connect_error", Err: err,
			}); emitErr != nil {
				return emitErr
			}
			if !reconnectable(err) {
				return err
			}
			if err := wait(ctx, backoff(failures)); err != nil {
				return r.shutdown(connections)
			}
			continue
		}
		if session.Recv == nil {
			cancel()
			return errors.New("stream receive function is required")
		}

		if len(replayRequests) > 0 && session.Send == nil {
			cancel()
			return errors.New("stream send function is required for registered subscriptions")
		}
		replayFailed := false
		for _, request := range replayRequests {
			if err := replayLimiter.waitN(ctx, 1); err != nil {
				cancel()
				closeSession(session)
				if ctx.Err() != nil {
					return r.shutdown(connections)
				}
				return err
			}
			if err := session.Send(request); err != nil {
				failures++
				cancel()
				closeSession(session)
				if emitErr := r.emit(LifecycleEvent{
					Type: EventDisconnected, Time: time.Now().UTC(), Attempt: failures,
					Subscriptions: subscriptionCount, Reason: "resubscribe_error", Err: err,
				}); emitErr != nil {
					return emitErr
				}
				if !reconnectable(err) {
					return err
				}
				replayFailed = true
				break
			}
		}
		if replayFailed {
			cancel()
			closeSession(session)
			if err := wait(ctx, backoff(failures)); err != nil {
				return r.shutdown(connections)
			}
			continue
		}

		connections++
		if err := r.emit(LifecycleEvent{
			Type: EventConnected, Time: time.Now().UTC(), Attempt: connections,
			Subscriptions: subscriptionCount,
		}); err != nil {
			cancel()
			closeSession(session)
			return err
		}
		if connections > 1 {
			if err := r.emit(LifecycleEvent{
				Type: EventResubscribed, Time: time.Now().UTC(), Attempt: connections,
				Subscriptions: subscriptionCount,
			}); err != nil {
				cancel()
				closeSession(session)
				return err
			}
		}
		if ctx.Err() != nil {
			cancel()
			closeSession(session)
			return r.shutdown(connections)
		}
		reconnect, err := r.receive(
			ctx, streamCtx, cancel, session, watchdog, maxReconcileBuffer,
			connections, subscriptionCount, &failures,
		)
		if err != nil {
			return err
		}
		if !reconnect {
			return r.shutdown(connections)
		}
		if err := wait(ctx, backoff(failures)); err != nil {
			return r.shutdown(connections)
		}
	}
}

func (r Runner[Request, Response]) receive(
	ctx context.Context,
	streamCtx context.Context,
	cancel context.CancelFunc,
	session Session[Request, Response],
	watchdog time.Duration,
	maxReconcileBuffer int,
	connection, subscriptions int,
	failures *int,
) (bool, error) {
	timer := time.NewTimer(watchdog)
	defer timer.Stop()
	connectedAt := time.Now()
	results := make(chan receiveResult[Response], 1)
	go func() {
		for {
			message, err := session.Recv()
			select {
			case results <- receiveResult[Response]{message: message, err: err}:
			case <-streamCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	var reconcileDone <-chan error
	if r.Reconcile != nil {
		done := make(chan error, 1)
		reconcileDone = done
		go func() { done <- r.Reconcile(streamCtx) }()
	}
	reconciling := reconcileDone != nil
	buffered := make([]*Response, 0)
	stop := func() {
		cancel()
		closeSession(session)
		if reconcileDone != nil {
			<-reconcileDone
			reconcileDone = nil
		}
	}
	for {
		select {
		case <-ctx.Done():
			stop()
			return false, nil
		case reconcileErr := <-reconcileDone:
			reconcileDone = nil
			reconciling = false
			if reconcileErr != nil {
				stop()
				if ctx.Err() != nil || errors.Is(reconcileErr, context.Canceled) {
					return false, nil
				}
				*failures++
				if err := r.emit(LifecycleEvent{
					Type: EventDisconnected, Time: time.Now().UTC(), Attempt: connection,
					Subscriptions: subscriptions, Reason: "reconcile_error", Err: reconcileErr,
				}); err != nil {
					return false, err
				}
				if !reconnectable(reconcileErr) {
					return false, reconcileErr
				}
				return true, nil
			}
			if ctx.Err() != nil {
				stop()
				return false, nil
			}
			for _, message := range buffered {
				if err := r.deliverBuffered(message); err != nil {
					stop()
					return false, err
				}
				if ctx.Err() != nil {
					stop()
					return false, nil
				}
			}
			buffered = nil
		case received := <-results:
			if received.err != nil {
				stop()
				if ctx.Err() != nil || errors.Is(received.err, context.Canceled) {
					return false, nil
				}
				if time.Since(connectedAt) >= watchdog {
					*failures = 0
				}
				*failures++
				if err := r.emit(LifecycleEvent{
					Type: EventDisconnected, Time: time.Now().UTC(), Attempt: connection,
					Subscriptions: subscriptions, Reason: streamErrorReason(received.err), Err: received.err,
				}); err != nil {
					return false, err
				}
				if !reconnectable(received.err) {
					return false, received.err
				}
				return true, nil
			}
			if r.IsActivity == nil || r.IsActivity(received.message) {
				resetTimer(timer, watchdog)
			}
			if reconciling && (r.BufferDuringReconcile == nil || r.BufferDuringReconcile(received.message)) {
				if len(buffered) >= maxReconcileBuffer {
					stop()
					*failures++
					if err := r.emit(LifecycleEvent{
						Type: EventLagging, Time: time.Now().UTC(), Attempt: connection,
						Subscriptions: subscriptions, Reason: "reconcile_buffer_overflow",
					}); err != nil {
						return false, err
					}
					if err := r.emit(LifecycleEvent{
						Type: EventDisconnected, Time: time.Now().UTC(), Attempt: connection,
						Subscriptions: subscriptions, Reason: "reconcile_buffer_overflow",
					}); err != nil {
						return false, err
					}
					return true, nil
				}
				buffered = append(buffered, received.message)
				continue
			}
			if err := r.deliverLive(received.message, !reconciling); err != nil {
				stop()
				return false, err
			}
		case <-timer.C:
			stop()
			if err := r.emit(LifecycleEvent{
				Type: EventLagging, Time: time.Now().UTC(), Attempt: connection,
				Subscriptions: subscriptions, Reason: "watchdog_timeout",
			}); err != nil {
				return false, err
			}
			*failures++
			if err := r.emit(LifecycleEvent{
				Type: EventDisconnected, Time: time.Now().UTC(), Attempt: connection,
				Subscriptions: subscriptions, Reason: "watchdog_timeout",
			}); err != nil {
				return false, err
			}
			return true, nil
		}
	}
}

func (r Runner[Request, Response]) deliverBuffered(message *Response) error {
	if r.KeepAfterReconcile != nil && !r.KeepAfterReconcile(message) {
		return nil
	}
	return r.deliver(message)
}

func (r Runner[Request, Response]) deliverLive(message *Response, reconciled bool) error {
	if reconciled && r.KeepLiveAfterReconcile != nil && !r.KeepLiveAfterReconcile(message) {
		return nil
	}
	return r.deliver(message)
}

func (r Runner[Request, Response]) deliver(message *Response) error {
	if r.OnMessage == nil {
		return nil
	}
	return r.OnMessage(message)
}

func (r Runner[Request, Response]) shutdown(connections int) error {
	return r.emit(LifecycleEvent{
		Type: EventDisconnected, Time: time.Now().UTC(), Attempt: connections,
		Reason: "shutdown", Final: true,
	})
}

func (r Runner[Request, Response]) emit(event LifecycleEvent) error {
	if r.OnLifecycle == nil {
		return nil
	}
	return r.OnLifecycle(event)
}

func closeSession[Request, Response any](session Session[Request, Response]) {
	if session.CloseSend != nil {
		_ = session.CloseSend()
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func streamErrorReason(err error) string {
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	return "stream_error"
}

func reconnectable(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument,
		codes.FailedPrecondition, codes.NotFound, codes.Unimplemented:
		return false
	default:
		return true
	}
}
