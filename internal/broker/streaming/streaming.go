// Package streaming adapts the T-Invest stream services to the generic
// resilient runner and builds de-duplicated market-data subscription requests.
package streaming

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"tinvest/internal/broker/marketdata"
	investapi "tinvest/internal/pb/investapi"
	streamrunner "tinvest/internal/stream"
)

const (
	DefaultPingDelay = 10 * time.Second
	MaxSubscriptions = 300
)

// ServerRequest is the unused request type for server-stream runner sessions.
type ServerRequest struct{}

type MarketDataOptions struct {
	CandleInterval string
	OrderBookDepth int32
	Trades         bool
	LastPrice      bool
	Info           bool
}

// Client owns the typed stream and reconciliation RPC clients on one shared
// transport connection.
type Client struct {
	marketStream investapi.MarketDataStreamServiceClient
	market       investapi.MarketDataServiceClient
	operations   investapi.OperationsStreamServiceClient
	orders       investapi.OrdersStreamServiceClient
}

func New(cc grpc.ClientConnInterface) Client {
	return Client{
		marketStream: investapi.NewMarketDataStreamServiceClient(cc),
		market:       investapi.NewMarketDataServiceClient(cc),
		operations:   investapi.NewOperationsStreamServiceClient(cc),
		orders:       investapi.NewOrdersStreamServiceClient(cc),
	}
}

func (c Client) OpenMarketData(ctx context.Context) (streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse], error) {
	client, err := c.marketStream.MarketDataStream(ctx)
	if err != nil {
		return streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse]{}, err
	}
	return streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse]{
		Recv: client.Recv, Send: client.Send, CloseSend: client.CloseSend,
	}, nil
}

func (c Client) OpenPortfolio(ctx context.Context, accountID string) (streamrunner.Session[ServerRequest, investapi.PortfolioStreamResponse], error) {
	client, err := c.operations.PortfolioStream(ctx, &investapi.PortfolioStreamRequest{
		Accounts: []string{accountID}, PingSettings: pingSettings(),
	})
	if err != nil {
		return streamrunner.Session[ServerRequest, investapi.PortfolioStreamResponse]{}, err
	}
	return streamrunner.Session[ServerRequest, investapi.PortfolioStreamResponse]{
		Recv: client.Recv, CloseSend: client.CloseSend,
	}, nil
}

func (c Client) OpenPositions(ctx context.Context, accountID string) (streamrunner.Session[ServerRequest, investapi.PositionsStreamResponse], error) {
	client, err := c.operations.PositionsStream(ctx, &investapi.PositionsStreamRequest{
		Accounts: []string{accountID}, WithInitialPositions: true, PingSettings: pingSettings(),
	})
	if err != nil {
		return streamrunner.Session[ServerRequest, investapi.PositionsStreamResponse]{}, err
	}
	return streamrunner.Session[ServerRequest, investapi.PositionsStreamResponse]{
		Recv: client.Recv, CloseSend: client.CloseSend,
	}, nil
}

func (c Client) OpenOrders(ctx context.Context, accountID string) (streamrunner.Session[ServerRequest, investapi.TradesStreamResponse], error) {
	delay := int32(DefaultPingDelay / time.Millisecond)
	client, err := c.orders.TradesStream(ctx, &investapi.TradesStreamRequest{
		Accounts: []string{accountID}, PingDelayMs: &delay,
	})
	if err != nil {
		return streamrunner.Session[ServerRequest, investapi.TradesStreamResponse]{}, err
	}
	return streamrunner.Session[ServerRequest, investapi.TradesStreamResponse]{
		Recv: client.Recv, CloseSend: client.CloseSend,
	}, nil
}

// OrderBookSnapshot fetches the authoritative unary state used after every
// market-data connection and reconnect.
func (c Client) OrderBookSnapshot(ctx context.Context, instrumentID string, depth int32) (*investapi.GetOrderBookResponse, error) {
	if err := marketdata.ValidateDepth(depth); err != nil {
		return nil, err
	}
	return c.market.GetOrderBook(ctx, &investapi.GetOrderBookRequest{InstrumentId: &instrumentID, Depth: depth})
}

// Activity classifiers implement the watchdog's data-or-ping contract while
// deliberately ignoring subscription acknowledgement/control frames.
func MarketDataActivity(response *investapi.MarketDataResponse) bool {
	return response != nil && (response.GetPing() != nil || response.GetCandle() != nil ||
		response.GetTrade() != nil || response.GetOrderbook() != nil || response.GetTradingStatus() != nil ||
		response.GetLastPrice() != nil || response.GetOpenInterest() != nil)
}

func PortfolioActivity(response *investapi.PortfolioStreamResponse) bool {
	return response != nil && (response.GetPing() != nil || response.GetPortfolio() != nil)
}

func PositionsActivity(response *investapi.PositionsStreamResponse) bool {
	return response != nil && (response.GetPing() != nil || response.GetPosition() != nil || response.GetInitialPositions() != nil)
}

func OrdersActivity(response *investapi.TradesStreamResponse) bool {
	return response != nil && (response.GetPing() != nil || response.GetOrderTrades() != nil)
}

// MarketDataSubscriptions builds one request per selected data shape, plus
// ping settings. Grouping all instruments into each request respects the
// subscribe-request/minute cap and makes replay deterministic.
func MarketDataSubscriptions(instrumentIDs []string, options MarketDataOptions) (*streamrunner.Registry[investapi.MarketDataRequest], error) {
	instrumentIDs = UniqueInstrumentIDs(instrumentIDs)
	if len(instrumentIDs) == 0 {
		return nil, fmt.Errorf("at least one --instrument is required")
	}
	if options.CandleInterval == "" && options.OrderBookDepth == 0 && !options.Trades && !options.LastPrice && !options.Info {
		return nil, fmt.Errorf("select at least one of --candles, --orderbook, --trades, --last-price, or --info")
	}
	kinds := 0
	if options.CandleInterval != "" {
		kinds++
	}
	if options.OrderBookDepth != 0 {
		kinds++
	}
	if options.Trades {
		kinds++
	}
	if options.LastPrice {
		kinds++
	}
	if options.Info {
		kinds++
	}
	logicalSubscriptions := len(instrumentIDs) * kinds
	if logicalSubscriptions > MaxSubscriptions {
		return nil, fmt.Errorf("too many subscriptions: %d exceeds stream cap %d", logicalSubscriptions, MaxSubscriptions)
	}
	registry := streamrunner.NewRegistry[investapi.MarketDataRequest]()
	action := investapi.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE
	if options.CandleInterval != "" {
		interval, err := ParseSubscriptionInterval(options.CandleInterval)
		if err != nil {
			return nil, err
		}
		instruments := make([]*investapi.CandleInstrument, 0, len(instrumentIDs))
		for _, id := range instrumentIDs {
			instruments = append(instruments, &investapi.CandleInstrument{InstrumentId: id, Interval: interval})
		}
		registry.Add("candles", &investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_SubscribeCandlesRequest{
			SubscribeCandlesRequest: &investapi.SubscribeCandlesRequest{SubscriptionAction: action, Instruments: instruments},
		}})
	}
	if options.OrderBookDepth != 0 {
		if err := marketdata.ValidateDepth(options.OrderBookDepth); err != nil {
			return nil, err
		}
		instruments := make([]*investapi.OrderBookInstrument, 0, len(instrumentIDs))
		for _, id := range instrumentIDs {
			instruments = append(instruments, &investapi.OrderBookInstrument{InstrumentId: id, Depth: options.OrderBookDepth})
		}
		registry.Add("orderbook", &investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_SubscribeOrderBookRequest{
			SubscribeOrderBookRequest: &investapi.SubscribeOrderBookRequest{SubscriptionAction: action, Instruments: instruments},
		}})
	}
	if options.Trades {
		instruments := make([]*investapi.TradeInstrument, 0, len(instrumentIDs))
		for _, id := range instrumentIDs {
			instruments = append(instruments, &investapi.TradeInstrument{InstrumentId: id})
		}
		registry.Add("trades", &investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_SubscribeTradesRequest{
			SubscribeTradesRequest: &investapi.SubscribeTradesRequest{SubscriptionAction: action, Instruments: instruments},
		}})
	}
	if options.LastPrice {
		instruments := make([]*investapi.LastPriceInstrument, 0, len(instrumentIDs))
		for _, id := range instrumentIDs {
			instruments = append(instruments, &investapi.LastPriceInstrument{InstrumentId: id})
		}
		registry.Add("last-price", &investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_SubscribeLastPriceRequest{
			SubscribeLastPriceRequest: &investapi.SubscribeLastPriceRequest{SubscriptionAction: action, Instruments: instruments},
		}})
	}
	if options.Info {
		instruments := make([]*investapi.InfoInstrument, 0, len(instrumentIDs))
		for _, id := range instrumentIDs {
			instruments = append(instruments, &investapi.InfoInstrument{InstrumentId: id})
		}
		registry.Add("info", &investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_SubscribeInfoRequest{
			SubscribeInfoRequest: &investapi.SubscribeInfoRequest{SubscriptionAction: action, Instruments: instruments},
		}})
	}
	registry.Add("ping-settings", &investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_PingSettings{PingSettings: pingSettings()}})
	return registry, nil
}

// UniqueInstrumentIDs removes repeated resolved UIDs without disturbing the
// caller's order. Different user aliases can resolve to the same UID, so this
// must run after resolution as well as inside the request builder.
func UniqueInstrumentIDs(instrumentIDs []string) []string {
	seen := make(map[string]struct{}, len(instrumentIDs))
	unique := make([]string, 0, len(instrumentIDs))
	for _, id := range instrumentIDs {
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

func ParseSubscriptionInterval(raw string) (investapi.SubscriptionInterval, error) {
	intervals := map[string]investapi.SubscriptionInterval{
		"1m":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE,
		"2m":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_2_MIN,
		"3m":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_3_MIN,
		"5m":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_FIVE_MINUTES,
		"10m": investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_10_MIN,
		"15m": investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTES,
		"30m": investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_30_MIN,
		"1h":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_HOUR,
		"2h":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_2_HOUR,
		"4h":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_4_HOUR,
		"1d":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_DAY,
		"1w":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_WEEK,
		"1M":  investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_MONTH,
	}
	interval, ok := intervals[raw]
	if !ok {
		return investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_UNSPECIFIED,
			fmt.Errorf("invalid candle interval %q: want 1m, 2m, 3m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 1d, 1w, or 1M", raw)
	}
	return interval, nil
}

func pingSettings() *investapi.PingDelaySettings {
	delay := int32(DefaultPingDelay / time.Millisecond)
	return &investapi.PingDelaySettings{PingDelayMs: &delay}
}
