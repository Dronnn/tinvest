package streaming

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	investapi "tinvest/internal/pb/investapi"
)

type requestShapeServer struct {
	investapi.UnimplementedOperationsStreamServiceServer
	investapi.UnimplementedOrdersStreamServiceServer
	portfolio chan *investapi.PortfolioStreamRequest
	positions chan *investapi.PositionsStreamRequest
	orders    chan *investapi.TradesStreamRequest
}

func (s *requestShapeServer) PortfolioStream(request *investapi.PortfolioStreamRequest, stream grpc.ServerStreamingServer[investapi.PortfolioStreamResponse]) error {
	s.portfolio <- request
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *requestShapeServer) PositionsStream(request *investapi.PositionsStreamRequest, stream grpc.ServerStreamingServer[investapi.PositionsStreamResponse]) error {
	s.positions <- request
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *requestShapeServer) TradesStream(request *investapi.TradesStreamRequest, stream grpc.ServerStreamingServer[investapi.TradesStreamResponse]) error {
	s.orders <- request
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestMarketDataSubscriptionsContainEachRequestedShapeOnce(t *testing.T) {
	registry, err := MarketDataSubscriptions([]string{"uid-1", "uid-2"}, MarketDataOptions{
		CandleInterval: "5m", OrderBookDepth: 20, Trades: true, LastPrice: true, Info: true,
	})
	if err != nil {
		t.Fatalf("MarketDataSubscriptions: %v", err)
	}
	requests := registry.Snapshot()
	if len(requests) != 6 {
		t.Fatalf("requests = %d, want five subscriptions plus ping settings", len(requests))
	}
	counts := map[string]int{}
	for _, request := range requests {
		switch {
		case request.GetSubscribeCandlesRequest() != nil:
			counts["candles"]++
			candles := request.GetSubscribeCandlesRequest().GetInstruments()
			if len(candles) != 2 || candles[0].GetInterval() != investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_FIVE_MINUTES {
				t.Fatalf("candle instruments = %+v", candles)
			}
		case request.GetSubscribeOrderBookRequest() != nil:
			counts["orderbook"]++
		case request.GetSubscribeTradesRequest() != nil:
			counts["trades"]++
		case request.GetSubscribeLastPriceRequest() != nil:
			counts["last_price"]++
		case request.GetSubscribeInfoRequest() != nil:
			counts["info"]++
		case request.GetPingSettings() != nil:
			counts["ping_settings"]++
			if got := time.Duration(request.GetPingSettings().GetPingDelayMs()) * time.Millisecond; got != DefaultPingDelay {
				t.Fatalf("ping delay = %s, want %s", got, DefaultPingDelay)
			}
		default:
			t.Fatalf("unexpected request: %v", request)
		}
	}
	for _, kind := range []string{"candles", "orderbook", "trades", "last_price", "info", "ping_settings"} {
		if counts[kind] != 1 {
			t.Errorf("%s requests = %d, want 1", kind, counts[kind])
		}
	}
}

func TestActivityClassifiersIgnoreSubscriptionAcknowledgements(t *testing.T) {
	marketAck := &investapi.MarketDataResponse{Payload: &investapi.MarketDataResponse_SubscribeTradesResponse{
		SubscribeTradesResponse: &investapi.SubscribeTradesResponse{},
	}}
	if MarketDataActivity(marketAck) {
		t.Fatal("market-data subscription acknowledgement is not data/ping activity")
	}
	if !MarketDataActivity(&investapi.MarketDataResponse{Payload: &investapi.MarketDataResponse_Ping{Ping: &investapi.Ping{}}}) {
		t.Fatal("market-data ping must reset the watchdog")
	}
	if PortfolioActivity(&investapi.PortfolioStreamResponse{Payload: &investapi.PortfolioStreamResponse_Subscriptions{
		Subscriptions: &investapi.PortfolioSubscriptionResult{},
	}}) {
		t.Fatal("portfolio subscription acknowledgement is not data/ping activity")
	}
	if !PositionsActivity(&investapi.PositionsStreamResponse{Payload: &investapi.PositionsStreamResponse_InitialPositions{
		InitialPositions: &investapi.PositionsResponse{},
	}}) {
		t.Fatal("initial positions snapshot is data activity")
	}
}

func TestServerStreamRequestShapes(t *testing.T) {
	serverImplementation := &requestShapeServer{
		portfolio: make(chan *investapi.PortfolioStreamRequest, 1),
		positions: make(chan *investapi.PositionsStreamRequest, 1),
		orders:    make(chan *investapi.TradesStreamRequest, 1),
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	investapi.RegisterOperationsStreamServiceServer(server, serverImplementation)
	investapi.RegisterOrdersStreamServiceServer(server, serverImplementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := New(connection)

	portfolioCtx, cancelPortfolio := context.WithCancel(context.Background())
	portfolioSession, err := client.OpenPortfolio(portfolioCtx, "account-1")
	if err != nil {
		t.Fatalf("OpenPortfolio: %v", err)
	}
	portfolio := <-serverImplementation.portfolio
	if len(portfolio.GetAccounts()) != 1 || portfolio.GetAccounts()[0] != "account-1" || portfolio.GetPingSettings() == nil {
		t.Fatalf("portfolio request = %+v", portfolio)
	}
	cancelPortfolio()
	if portfolioSession.CloseSend != nil {
		_ = portfolioSession.CloseSend()
	}

	positionsCtx, cancelPositions := context.WithCancel(context.Background())
	positionsSession, err := client.OpenPositions(positionsCtx, "account-1")
	if err != nil {
		t.Fatalf("OpenPositions: %v", err)
	}
	positions := <-serverImplementation.positions
	if len(positions.GetAccounts()) != 1 || positions.GetAccounts()[0] != "account-1" ||
		!positions.GetWithInitialPositions() || positions.GetPingSettings() == nil {
		t.Fatalf("positions request = %+v", positions)
	}
	cancelPositions()
	if positionsSession.CloseSend != nil {
		_ = positionsSession.CloseSend()
	}

	ordersCtx, cancelOrders := context.WithCancel(context.Background())
	ordersSession, err := client.OpenOrders(ordersCtx, "account-1")
	if err != nil {
		t.Fatalf("OpenOrders: %v", err)
	}
	orders := <-serverImplementation.orders
	if len(orders.GetAccounts()) != 1 || orders.GetAccounts()[0] != "account-1" ||
		time.Duration(orders.GetPingDelayMs())*time.Millisecond != DefaultPingDelay {
		t.Fatalf("orders request = %+v", orders)
	}
	cancelOrders()
	if ordersSession.CloseSend != nil {
		_ = ordersSession.CloseSend()
	}
}

func TestMarketDataSubscriptionsRejectNoDataFlags(t *testing.T) {
	if _, err := MarketDataSubscriptions([]string{"uid-1"}, MarketDataOptions{}); err == nil {
		t.Fatal("want error when no subscription kind is selected")
	}
}

func TestMarketDataSubscriptionsDeduplicateAndEnforceLogicalCap(t *testing.T) {
	registry, err := MarketDataSubscriptions([]string{"uid-1", "uid-1", "uid-2"}, MarketDataOptions{
		Trades: true, LastPrice: true,
	})
	if err != nil {
		t.Fatalf("MarketDataSubscriptions: %v", err)
	}
	requests := registry.Snapshot()
	if got := len(requests[0].GetSubscribeTradesRequest().GetInstruments()); got != 2 {
		t.Fatalf("deduplicated trade instruments = %d, want 2", got)
	}

	ids := make([]string, 61)
	for i := range ids {
		ids[i] = "uid-" + strconv.Itoa(i)
	}
	_, err = MarketDataSubscriptions(ids, MarketDataOptions{
		CandleInterval: "1m", OrderBookDepth: 20, Trades: true, LastPrice: true, Info: true,
	})
	if err == nil {
		t.Fatal("want 305 logical subscriptions to exceed the 300 cap")
	}
}

func TestParseSubscriptionInterval(t *testing.T) {
	for _, raw := range []string{"1m", "2m", "3m", "5m", "10m", "15m", "30m", "1h", "2h", "4h", "1d", "1w", "1M"} {
		if _, err := ParseSubscriptionInterval(raw); err != nil {
			t.Errorf("ParseSubscriptionInterval(%q): %v", raw, err)
		}
	}
	if _, err := ParseSubscriptionInterval("7m"); err == nil {
		t.Fatal("want invalid interval error")
	}
}
