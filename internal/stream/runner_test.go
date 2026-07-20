package stream_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	streamrunner "github.com/Dronnn/tinvest/internal/stream"
	"github.com/Dronnn/tinvest/internal/transport"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type chaosMarketDataServer struct {
	investapi.UnimplementedMarketDataStreamServiceServer
	investapi.UnimplementedMarketDataServiceServer

	mu          sync.Mutex
	connections int
	requests    [][]*investapi.MarketDataRequest
	snapshots   int
	silent      bool
	received    chan int
}

func TestReconcileFiltersBufferedFramesBeforeDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values := []int{1, 2}
	index := 0
	delivered := make([]int, 0, 1)
	runner := streamrunner.Runner[int, int]{
		Open: func(streamCtx context.Context) (streamrunner.Session[int, int], error) {
			return streamrunner.Session[int, int]{Recv: func() (*int, error) {
				if index < len(values) {
					value := values[index]
					index++
					return &value, nil
				}
				<-streamCtx.Done()
				return nil, streamCtx.Err()
			}}, nil
		},
		Reconcile: func(context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		KeepAfterReconcile: func(value *int) bool { return *value > 1 },
		OnMessage: func(value *int) error {
			delivered = append(delivered, *value)
			cancel()
			return nil
		},
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(delivered) != 1 || delivered[0] != 2 {
		t.Fatalf("delivered = %v, want only the post-snapshot frame", delivered)
	}
}

func TestWatchdogRunsWhileReconciliationIsPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connections := 0
	lagging := 0
	runner := streamrunner.Runner[int, int]{
		Open: func(streamCtx context.Context) (streamrunner.Session[int, int], error) {
			connections++
			return streamrunner.Session[int, int]{Recv: func() (*int, error) {
				<-streamCtx.Done()
				return nil, streamCtx.Err()
			}}, nil
		},
		Watchdog: 20 * time.Millisecond,
		Backoff:  func(int) time.Duration { return 0 },
		Reconcile: func(reconcileCtx context.Context) error {
			<-reconcileCtx.Done()
			return reconcileCtx.Err()
		},
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			if event.Type == streamrunner.EventLagging {
				lagging++
			}
			if event.Type == streamrunner.EventConnected && connections == 2 {
				cancel()
			}
			return nil
		},
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if lagging == 0 || connections < 2 {
		t.Fatalf("lagging = %d, connections = %d; want watchdog reconnect during reconciliation", lagging, connections)
	}
}

func TestShutdownWaitsForCanceledReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	finished := make(chan struct{})
	finalAfterJoin := false
	runner := streamrunner.Runner[int, int]{
		Open: func(streamCtx context.Context) (streamrunner.Session[int, int], error) {
			return streamrunner.Session[int, int]{Recv: func() (*int, error) {
				<-streamCtx.Done()
				return nil, streamCtx.Err()
			}}, nil
		},
		Reconcile: func(reconcileCtx context.Context) error {
			close(started)
			<-reconcileCtx.Done()
			time.Sleep(10 * time.Millisecond)
			close(finished)
			return reconcileCtx.Err()
		},
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			if event.Final {
				select {
				case <-finished:
					finalAfterJoin = true
				default:
				}
			}
			return nil
		},
	}
	go func() {
		<-started
		cancel()
	}()
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !finalAfterJoin {
		t.Fatal("final shutdown event was emitted before reconciliation stopped")
	}
}

func TestFlappingStreamEscalatesBackoffAfterReceivingOneFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connections := 0
	attempts := make([]int, 0, 3)
	runner := streamrunner.Runner[int, int]{
		Open: func(context.Context) (streamrunner.Session[int, int], error) {
			connections++
			delivered := false
			return streamrunner.Session[int, int]{Recv: func() (*int, error) {
				if !delivered {
					delivered = true
					value := connections
					return &value, nil
				}
				return nil, status.Error(codes.Unavailable, "flap")
			}}, nil
		},
		Backoff: func(attempt int) time.Duration {
			attempts = append(attempts, attempt)
			return 0
		},
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			if event.Type == streamrunner.EventConnected && connections == 4 {
				cancel()
			}
			return nil
		},
	}
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if len(attempts) < 3 || attempts[0] != 1 || attempts[1] != 2 || attempts[2] != 3 {
		t.Fatalf("backoff attempts = %v, want escalating 1,2,3", attempts)
	}
}

func TestReplayLimiterCapsSubscriptionRequestsAcrossReconnects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	registry := streamrunner.NewRegistry[int]()
	one, two := 1, 2
	registry.Add("one", &one)
	registry.Add("two", &two)
	connections := 0
	started := time.Now()
	runner := streamrunner.Runner[int, int]{
		Open: func(context.Context) (streamrunner.Session[int, int], error) {
			connections++
			return streamrunner.Session[int, int]{
				Send: func(*int) error { return nil },
				Recv: func() (*int, error) { return nil, status.Error(codes.Unavailable, "drop") },
			}, nil
		},
		Subscriptions: registry,
		ReplayLimit:   2,
		ReplayWindow:  50 * time.Millisecond,
		Backoff:       func(int) time.Duration { return 0 },
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			if event.Type == streamrunner.EventConnected && connections == 2 {
				cancel()
			}
			return nil
		},
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 45*time.Millisecond {
		t.Fatalf("second replay started after %v, want 2 requests per 50ms cap", elapsed)
	}
}

func TestLifecycleCountExcludesReplayableControlRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := streamrunner.NewRegistry[int]()
	subscription, control := 1, 2
	registry.Add("last-price", &subscription)
	registry.AddControl("ping-settings", &control)
	sent := 0
	connectedSubscriptions := -1
	runner := streamrunner.Runner[int, int]{
		Open: func(streamCtx context.Context) (streamrunner.Session[int, int], error) {
			return streamrunner.Session[int, int]{
				Send: func(*int) error {
					sent++
					return nil
				},
				Recv: func() (*int, error) {
					<-streamCtx.Done()
					return nil, streamCtx.Err()
				},
			}, nil
		},
		Subscriptions: registry,
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			if event.Type == streamrunner.EventConnected {
				connectedSubscriptions = event.Subscriptions
				cancel()
			}
			return nil
		},
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sent != 2 {
		t.Fatalf("replayed requests = %d, want subscription plus control request", sent)
	}
	if connectedSubscriptions != 1 {
		t.Fatalf("connected subscriptions = %d, want 1", connectedSubscriptions)
	}
}

func TestReplaySnapshotPairsRequestsWithSubscriptionCount(t *testing.T) {
	registry := streamrunner.NewRegistry[int]()
	subscription, control := 1, 2
	registry.Add("last-price", &subscription)
	registry.AddControl("ping-settings", &control)

	requests, subscriptionCount := registry.ReplaySnapshot()
	if len(requests) != 2 {
		t.Fatalf("replay requests = %d, want 2", len(requests))
	}
	if subscriptionCount != 1 {
		t.Fatalf("subscription request batches = %d, want 1", subscriptionCount)
	}
}

func (s *chaosMarketDataServer) MarketDataStream(stream grpc.BidiStreamingServer[investapi.MarketDataRequest, investapi.MarketDataResponse]) error {
	s.mu.Lock()
	connection := s.connections
	s.connections++
	s.requests = append(s.requests, nil)
	s.mu.Unlock()
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.requests[connection] = append(s.requests[connection], proto.Clone(request).(*investapi.MarketDataRequest))
	s.mu.Unlock()
	if s.received != nil {
		s.received <- connection
	}

	if s.silent {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	if connection == 0 {
		if err := stream.Send(&investapi.MarketDataResponse{Payload: &investapi.MarketDataResponse_Candle{
			Candle: &investapi.Candle{InstrumentUid: "uid-1"},
		}}); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "injected drop")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *chaosMarketDataServer) GetOrderBook(context.Context, *investapi.GetOrderBookRequest) (*investapi.GetOrderBookResponse, error) {
	s.mu.Lock()
	s.snapshots++
	s.mu.Unlock()
	return &investapi.GetOrderBookResponse{}, nil
}

func startChaosServer(t *testing.T, server *chaosMarketDataServer) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	serverOpt, clientCreds := bufTLS(t)
	grpcServer := grpc.NewServer(serverOpt)
	investapi.RegisterMarketDataStreamServiceServer(grpcServer, server)
	investapi.RegisterMarketDataServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := transport.Dial(context.Background(), transport.Config{
		Endpoint: "passthrough:///bufnet", Token: "test", Credentials: clientCreds,
	}, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func marketDataSession(client investapi.MarketDataStreamServiceClient) streamrunner.OpenFunc[investapi.MarketDataRequest, investapi.MarketDataResponse] {
	return func(ctx context.Context) (streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse], error) {
		stream, err := client.MarketDataStream(ctx)
		if err != nil {
			return streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse]{}, err
		}
		return streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse]{
			Recv: stream.Recv, Send: stream.Send, CloseSend: stream.CloseSend,
		}, nil
	}
}

func candleSubscription() *investapi.MarketDataRequest {
	return &investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_SubscribeCandlesRequest{
		SubscribeCandlesRequest: &investapi.SubscribeCandlesRequest{
			SubscriptionAction: investapi.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE,
			Instruments: []*investapi.CandleInstrument{{
				InstrumentId: "uid-1", Interval: investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE,
			}},
		},
	}}
}

func TestDropReconnectsAndReplaysDeduplicatedSubscription(t *testing.T) {
	server := &chaosMarketDataServer{received: make(chan int, 4)}
	conn := startChaosServer(t, server)
	client := investapi.NewMarketDataStreamServiceClient(conn)
	snapshotClient := investapi.NewMarketDataServiceClient(conn)
	registry := streamrunner.NewRegistry[investapi.MarketDataRequest]()
	registry.Add("candles:uid-1:1m", candleSubscription())
	registry.Add("candles:uid-1:1m", candleSubscription())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events := make(chan streamrunner.LifecycleEvent, 16)
	reconcileInvocations := 0
	reconciled := 0
	runner := streamrunner.Runner[investapi.MarketDataRequest, investapi.MarketDataResponse]{
		Open: marketDataSession(client), Subscriptions: registry, Watchdog: time.Second,
		Backoff: func(int) time.Duration { return 0 },
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			events <- event
			return nil
		},
		Reconcile: func(reconcileCtx context.Context) error {
			reconcileInvocations++
			_, err := snapshotClient.GetOrderBook(reconcileCtx, &investapi.GetOrderBookRequest{})
			if err != nil {
				// Connection 0's reconcile can be cancelled by connection 0's own
				// injected drop (Reconcile is bound to streamCtx by design). That
				// is expected; the next connection reconciles afresh.
				return err
			}
			reconciled++
			// Shut down after the first successful reconcile on a *reconnected*
			// connection. Keyed on the invocation count (one per connection), not
			// on the count of successful reconciles, so whether connection 0's
			// reconcile survived its drop cannot add a spurious third connection.
			if reconcileInvocations >= 2 {
				cancel()
			}
			return nil
		},
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	seen := map[streamrunner.EventType]bool{}
	final := false
	for len(events) > 0 {
		event := <-events
		seen[event.Type] = true
		final = final || event.Final
	}
	for _, eventType := range []streamrunner.EventType{
		streamrunner.EventConnected, streamrunner.EventDisconnected, streamrunner.EventResubscribed,
	} {
		if !seen[eventType] {
			t.Errorf("lifecycle event %q was not emitted", eventType)
		}
	}
	if !final {
		t.Error("final shutdown event was not emitted")
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if server.connections < 2 {
		t.Fatalf("connections = %d, want reconnect", server.connections)
	}
	// One reconcile per connection is the invariant. It is asserted on the
	// client-observed invocation count, not on server.snapshots: a reconcile
	// bound to streamCtx can be cancelled by its connection's drop after the
	// GetOrderBook request already reached the server, so the server-side count
	// races the drop (the source of this test's prior CI flake). The invocation
	// count does not — the runner calls Reconcile exactly once per connection.
	if reconcileInvocations != server.connections {
		t.Fatalf("reconcile invocations = %d, want one per connection (%d)", reconcileInvocations, server.connections)
	}
	if reconciled < 1 {
		t.Fatalf("no reconcile completed successfully after reconnect")
	}
	for connection, requests := range server.requests[:2] {
		if len(requests) != 1 {
			t.Fatalf("connection %d received %d subscriptions, want exactly one", connection, len(requests))
		}
		if !proto.Equal(requests[0], candleSubscription()) {
			t.Fatalf("connection %d replay mismatch: %v", connection, requests[0])
		}
	}
}

func TestSilencePastWatchdogTearsDownAndReconnects(t *testing.T) {
	server := &chaosMarketDataServer{silent: true, received: make(chan int, 4)}
	client := investapi.NewMarketDataStreamServiceClient(startChaosServer(t, server))
	registry := streamrunner.NewRegistry[investapi.MarketDataRequest]()
	registry.Add("candles", candleSubscription())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lagging := 0
	connected := 0
	runner := streamrunner.Runner[investapi.MarketDataRequest, investapi.MarketDataResponse]{
		Open: marketDataSession(client), Subscriptions: registry, Watchdog: 40 * time.Millisecond,
		Backoff: func(int) time.Duration { return 0 },
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			switch event.Type {
			case streamrunner.EventLagging:
				lagging++
			case streamrunner.EventConnected:
				connected++
				if connected == 2 {
					waitForServerConnection(t, server.received, 1)
					cancel()
				}
			}
			return nil
		},
	}
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if lagging == 0 {
		t.Fatal("lagging lifecycle event was not emitted")
	}
	server.mu.Lock()
	connections := server.connections
	server.mu.Unlock()
	if connections < 2 {
		t.Fatalf("connections = %d, want watchdog reconnect", connections)
	}
}

func TestNonRetryableOpenErrorStopsImmediately(t *testing.T) {
	calls := 0
	runner := streamrunner.Runner[investapi.MarketDataRequest, investapi.MarketDataResponse]{
		Open: func(context.Context) (streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse], error) {
			calls++
			return streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse]{}, status.Error(codes.Unauthenticated, "bad token")
		},
		Backoff: func(int) time.Duration { return 0 },
	}
	err := runner.Run(context.Background())
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Run error = %v, want Unauthenticated", err)
	}
	if calls != 1 {
		t.Fatalf("open calls = %d, want 1", calls)
	}
}

// TestCancelOnQuietConnectionExitsBeforeWatchdog locks in prompt shutdown: when
// the run context is cancelled while the runner sits on an established but silent
// connection, Run must return well within the watchdog window and must not
// redial. It guards against a regression where a cancelled run leaks a full
// watchdog wait and a spurious extra connection on the way out.
func TestCancelOnQuietConnectionExitsBeforeWatchdog(t *testing.T) {
	watchdog := 500 * time.Millisecond
	opens := 0
	connected := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := streamrunner.Runner[int, int]{
		Watchdog: watchdog,
		Backoff:  func(int) time.Duration { return 0 },
		Open: func(streamCtx context.Context) (streamrunner.Session[int, int], error) {
			opens++
			return streamrunner.Session[int, int]{Recv: func() (*int, error) {
				<-streamCtx.Done() // silent forever
				return nil, streamCtx.Err()
			}}, nil
		},
		OnLifecycle: func(event streamrunner.LifecycleEvent) error {
			if event.Type == streamrunner.EventConnected {
				select {
				case connected <- struct{}{}:
				default:
				}
			}
			return nil
		},
	}
	go func() {
		<-connected
		cancel()
	}()
	start := time.Now()
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > watchdog/2 {
		t.Fatalf("shutdown took %v, want well under the %v watchdog (no full-window wait)", elapsed, watchdog)
	}
	if opens != 1 {
		t.Fatalf("opens = %d, want exactly 1 (no redial after cancellation)", opens)
	}
}

func waitForServerConnection(t *testing.T, received <-chan int, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case connection := <-received:
			if connection >= want {
				return
			}
		case <-deadline.C:
			t.Fatalf("server never received replay on connection %d", want)
		}
	}
}
