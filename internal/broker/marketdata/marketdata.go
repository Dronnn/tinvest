// Package marketdata wraps the quote/orderbook/status surface of
// MarketDataService (plan §5/§8): last prices, close prices, order book, and
// trading status. Typed params in, typed results out — instrument
// resolution happens one layer up, in internal/broker/instruments.
package marketdata

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over the market-data calls.
type Client struct {
	api investapi.MarketDataServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewMarketDataServiceClient(cc)}
}

// ValidDepths are the order-book depths the broker accepts (plan §8).
var ValidDepths = [...]int32{1, 10, 20, 30, 40, 50}

// ValidateDepth rejects any depth outside ValidDepths before a call is made.
func ValidateDepth(depth int32) error {
	for _, d := range ValidDepths {
		if d == depth {
			return nil
		}
	}
	return fmt.Errorf("invalid orderbook depth %d: want one of 1, 10, 20, 30, 40, 50", depth)
}

// LastPrices returns the last trade price for each instrument id (uid,
// figi, or ticker_classcode — all accepted by the broker as instrument_id).
func (c Client) LastPrices(ctx context.Context, instrumentIDs []string) ([]*investapi.LastPrice, error) {
	resp, err := c.api.GetLastPrices(ctx, &investapi.GetLastPricesRequest{InstrumentId: instrumentIDs})
	if err != nil {
		return nil, err
	}
	return resp.GetLastPrices(), nil
}

// ClosePrices returns the trading-session close price for each instrument.
func (c Client) ClosePrices(ctx context.Context, instrumentIDs []string) ([]*investapi.InstrumentClosePriceResponse, error) {
	reqs := make([]*investapi.InstrumentClosePriceRequest, 0, len(instrumentIDs))
	for _, id := range instrumentIDs {
		reqs = append(reqs, &investapi.InstrumentClosePriceRequest{InstrumentId: id})
	}
	resp, err := c.api.GetClosePrices(ctx, &investapi.GetClosePricesRequest{Instruments: reqs})
	if err != nil {
		return nil, err
	}
	return resp.GetClosePrices(), nil
}

// OrderBook returns the order book for one instrument at the given depth.
// depth is validated locally before any network call.
func (c Client) OrderBook(ctx context.Context, instrumentID string, depth int32) (*investapi.GetOrderBookResponse, error) {
	if err := ValidateDepth(depth); err != nil {
		return nil, err
	}
	return c.api.GetOrderBook(ctx, &investapi.GetOrderBookRequest{InstrumentId: &instrumentID, Depth: depth})
}

// TradingStatus returns the current trading status for one instrument.
func (c Client) TradingStatus(ctx context.Context, instrumentID string) (*investapi.GetTradingStatusResponse, error) {
	return c.api.GetTradingStatus(ctx, &investapi.GetTradingStatusRequest{InstrumentId: &instrumentID})
}
