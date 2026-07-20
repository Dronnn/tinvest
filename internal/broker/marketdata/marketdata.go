// Package marketdata wraps the quote/orderbook/status surface of
// MarketDataService (plan §5/§8): last prices, close prices, order book, and
// trading status. Typed params in, typed results out — instrument
// resolution happens one layer up, in internal/broker/instruments.
package marketdata

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// Client is a thin typed wrapper over the market-data calls.
type Client struct {
	api   investapi.MarketDataServiceClient
	pause func(context.Context) error
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewMarketDataServiceClient(cc), pause: candleRequestPause}
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

// CandleWindow is one broker-safe request range. Adjacent windows are
// contiguous and together cover the exact requested range.
type CandleWindow struct {
	From time.Time
	To   time.Time
}

// ParseCandleInterval maps the CLI interval spelling to the protobuf enum.
func ParseCandleInterval(raw string) (investapi.CandleInterval, error) {
	intervals := map[string]investapi.CandleInterval{
		"1m":  investapi.CandleInterval_CANDLE_INTERVAL_1_MIN,
		"2m":  investapi.CandleInterval_CANDLE_INTERVAL_2_MIN,
		"3m":  investapi.CandleInterval_CANDLE_INTERVAL_3_MIN,
		"5m":  investapi.CandleInterval_CANDLE_INTERVAL_5_MIN,
		"10m": investapi.CandleInterval_CANDLE_INTERVAL_10_MIN,
		"15m": investapi.CandleInterval_CANDLE_INTERVAL_15_MIN,
		"30m": investapi.CandleInterval_CANDLE_INTERVAL_30_MIN,
		"1h":  investapi.CandleInterval_CANDLE_INTERVAL_HOUR,
		"2h":  investapi.CandleInterval_CANDLE_INTERVAL_2_HOUR,
		"4h":  investapi.CandleInterval_CANDLE_INTERVAL_4_HOUR,
		"1d":  investapi.CandleInterval_CANDLE_INTERVAL_DAY,
		"1w":  investapi.CandleInterval_CANDLE_INTERVAL_WEEK,
		"1M":  investapi.CandleInterval_CANDLE_INTERVAL_MONTH,
	}
	interval, ok := intervals[raw]
	if !ok {
		return investapi.CandleInterval_CANDLE_INTERVAL_UNSPECIFIED, fmt.Errorf("invalid candle interval %q: want 1m, 2m, 3m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 1d, 1w, or 1M", raw)
	}
	return interval, nil
}

type candleRangeCap struct {
	duration time.Duration
	months   int
	years    int
}

func (cap candleRangeCap) advance(value time.Time) time.Time {
	if cap.duration != 0 {
		return value.Add(cap.duration)
	}
	return value.AddDate(cap.years, cap.months, 0)
}

func candleCap(interval investapi.CandleInterval) (candleRangeCap, error) {
	switch interval {
	case investapi.CandleInterval_CANDLE_INTERVAL_5_SEC,
		investapi.CandleInterval_CANDLE_INTERVAL_10_SEC:
		// proto/marketdata.proto: 5s and 10s candles span up to 200 minutes.
		return candleRangeCap{duration: 200 * time.Minute}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_30_SEC:
		// proto/marketdata.proto: 30s candles span up to 20 hours.
		return candleRangeCap{duration: 20 * time.Hour}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_1_MIN,
		investapi.CandleInterval_CANDLE_INTERVAL_2_MIN,
		investapi.CandleInterval_CANDLE_INTERVAL_3_MIN:
		return candleRangeCap{duration: 24 * time.Hour}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_5_MIN,
		investapi.CandleInterval_CANDLE_INTERVAL_10_MIN:
		return candleRangeCap{duration: 7 * 24 * time.Hour}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_15_MIN,
		investapi.CandleInterval_CANDLE_INTERVAL_30_MIN:
		return candleRangeCap{duration: 21 * 24 * time.Hour}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_HOUR,
		investapi.CandleInterval_CANDLE_INTERVAL_2_HOUR,
		investapi.CandleInterval_CANDLE_INTERVAL_4_HOUR:
		return candleRangeCap{months: 3}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_DAY:
		return candleRangeCap{years: 6}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_WEEK:
		return candleRangeCap{years: 5}, nil
	case investapi.CandleInterval_CANDLE_INTERVAL_MONTH:
		return candleRangeCap{years: 10}, nil
	default:
		return candleRangeCap{}, fmt.Errorf("unsupported candle interval %s", interval.String())
	}
}

// CandleWindows splits a range using the interval-specific broker caps.
func CandleWindows(from, to time.Time, interval investapi.CandleInterval) ([]CandleWindow, error) {
	if !from.Before(to) {
		return nil, fmt.Errorf("candle --from must be before --to")
	}
	cap, err := candleCap(interval)
	if err != nil {
		return nil, err
	}
	windows := make([]CandleWindow, 0, 1)
	for start := from; start.Before(to); {
		end := cap.advance(start)
		if end.After(to) {
			end = to
		}
		windows = append(windows, CandleWindow{From: start, To: end})
		start = end
	}
	return windows, nil
}

// Candles auto-windows long ranges, pauses briefly between calls, and
// concatenates every response in request order. HistoricCandle values,
// including is_complete, pass through unchanged.
func (c Client) Candles(ctx context.Context, instrumentID string, interval investapi.CandleInterval, from, to time.Time) ([]*investapi.HistoricCandle, error) {
	windows, err := CandleWindows(from, to, interval)
	if err != nil {
		return nil, err
	}
	result := make([]*investapi.HistoricCandle, 0)
	for index, window := range windows {
		if index > 0 {
			if err := c.pause(ctx); err != nil {
				return nil, err
			}
		}
		response, err := c.api.GetCandles(ctx, &investapi.GetCandlesRequest{
			From: timestamppb.New(window.From), To: timestamppb.New(window.To),
			Interval: interval, InstrumentId: &instrumentID,
		})
		if err != nil {
			return nil, err
		}
		result = append(result, response.GetCandles()...)
	}
	return result, nil
}

func candleRequestPause(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
