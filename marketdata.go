package tinvest

import (
	"context"
	"errors"
	"time"

	brokermarketdata "github.com/Dronnn/tinvest/internal/broker/marketdata"
	"github.com/Dronnn/tinvest/internal/transport"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// ParseCandleInterval maps a CLI-style interval spelling ("1m", "5m", "1h",
// "1d", "1w", "1M", …) to the protobuf enum. Callers may also use the
// investapi.CandleInterval constants directly.
func ParseCandleInterval(raw string) (investapi.CandleInterval, error) {
	return brokermarketdata.ParseCandleInterval(raw)
}

// LastPrices returns the last trade price for each instrument — the equivalent
// of `tinvest quotes last`. Each identifier is resolved to its instrument_uid
// first, in order.
func (c *Client) LastPrices(ctx context.Context, ids ...string) ([]*investapi.LastPrice, error) {
	uids, err := c.resolveUIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.marketdata.LastPrices(ctx, uids)
	return list, apiErr(err, info)
}

// ClosePrices returns the trading-session close price for each instrument —
// the equivalent of `tinvest quotes close`. Each identifier is resolved to its
// instrument_uid first, in order.
func (c *Client) ClosePrices(ctx context.Context, ids ...string) ([]*investapi.InstrumentClosePriceResponse, error) {
	uids, err := c.resolveUIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.marketdata.ClosePrices(ctx, uids)
	return list, apiErr(err, info)
}

// OrderBook returns the order book for one instrument at the given depth — the
// equivalent of `tinvest orderbook get`. Valid depths are 1, 10, 20, 30, 40,
// and 50; depth is validated before any network call. The identifier is
// resolved to its instrument_uid first.
func (c *Client) OrderBook(ctx context.Context, id string, depth int32) (*investapi.GetOrderBookResponse, error) {
	if err := brokermarketdata.ValidateDepth(depth); err != nil {
		return nil, apiErr(err, nil)
	}
	uid, err := c.resolveUID(ctx, id)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	book, err := c.marketdata.OrderBook(ctx, uid, depth)
	return book, apiErr(err, info)
}

// TradingStatus returns the current trading and order availability for one
// instrument — the equivalent of `tinvest instruments trading-status`. The
// identifier is resolved to its instrument_uid first.
func (c *Client) TradingStatus(ctx context.Context, id string) (*investapi.GetTradingStatusResponse, error) {
	uid, err := c.resolveUID(ctx, id)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	status, err := c.marketdata.TradingStatus(ctx, uid)
	return status, apiErr(err, info)
}

// Candles returns historic candles for one instrument in [from, to] — the
// equivalent of `tinvest candles get`. Long ranges are split automatically
// into broker-safe windows per the interval, with each window's response
// concatenated in request order. The identifier is resolved to its
// instrument_uid first.
func (c *Client) Candles(ctx context.Context, id string, interval investapi.CandleInterval, from, to time.Time) ([]*investapi.HistoricCandle, error) {
	uid, err := c.resolveUID(ctx, id)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	candles, err := c.marketdata.Candles(ctx, uid, interval, from, to)
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		// Candles may span several windowed calls sharing one CallInfo. A
		// cancellation between windows is not attributable to any single broker
		// call, so it must not surface an earlier window's tracking id.
		return nil, apiErr(err, nil)
	}
	return candles, apiErr(err, info)
}
