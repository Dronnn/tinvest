package tinvest

import (
	"context"
	"time"

	brokerinstruments "github.com/Dronnn/tinvest/internal/broker/instruments"
	"github.com/Dronnn/tinvest/internal/transport"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// ListedInstrument is the reference-data projection returned by Instruments,
// shared by every per-type list.
type ListedInstrument struct {
	UID           string
	FIGI          string
	Ticker        string
	ClassCode     string
	Name          string
	Type          string
	Lot           int32
	Currency      string
	TradingStatus investapi.SecurityTradingStatus
}

func convertListed(l brokerinstruments.ListedInstrument) ListedInstrument {
	return ListedInstrument{
		UID:           l.UID,
		FIGI:          l.FIGI,
		Ticker:        l.Ticker,
		ClassCode:     l.ClassCode,
		Name:          l.Name,
		Type:          l.Type,
		Lot:           l.Lot,
		Currency:      l.Currency,
		TradingStatus: l.TradingStatus,
	}
}

// Resolve resolves an instrument identifier (an instrument_uid, a FIGI, or a
// TICKER@CLASSCODE pair) to its full reference record via GetInstrumentBy —
// the library equivalent of `tinvest instruments get`. Successful resolutions
// are cached locally unless the client was created with DisableCache. A
// malformed identifier is caught locally before any network call; a broker
// failure carries the tracking id — both as an *APIError.
func (c *Client) Resolve(ctx context.Context, id string) (*investapi.Instrument, error) {
	ctx, info := transport.WithCallInfo(ctx)
	inst, err := c.instruments.Resolve(ctx, id, false)
	if err != nil {
		return nil, apiErr(err, info)
	}
	return inst, nil
}

// Search runs a free-text instrument search via FindInstrument — the
// equivalent of `tinvest instruments search`.
func (c *Client) Search(ctx context.Context, query string) ([]*investapi.InstrumentShort, error) {
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.instruments.Find(ctx, query)
	return list, apiErr(err, info)
}

// Instruments lists base instruments of one type — the equivalent of
// `tinvest instruments list --type`. instrumentType is one of "share",
// "bond", "etf", "currency", "future", or "option". An unknown type returns a
// plain error without a network call.
func (c *Client) Instruments(ctx context.Context, instrumentType string) ([]ListedInstrument, error) {
	if err := brokerinstruments.ValidateType(instrumentType); err != nil {
		return nil, apiErr(err, nil)
	}
	ctx, info := transport.WithCallInfo(ctx)
	listed, err := c.instruments.List(ctx, instrumentType)
	if err != nil {
		return nil, apiErr(err, info)
	}
	out := make([]ListedInstrument, len(listed))
	for i, l := range listed {
		out[i] = convertListed(l)
	}
	return out, nil
}

// Dividends lists dividend events for one instrument in [from, to) — the
// equivalent of `tinvest instruments dividends`. The identifier is resolved to
// its instrument_uid first.
func (c *Client) Dividends(ctx context.Context, id string, from, to time.Time) ([]*investapi.Dividend, error) {
	uid, err := c.resolveUID(ctx, id)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.instruments.Dividends(ctx, uid, from, to)
	return list, apiErr(err, info)
}

// Coupons lists bond coupon events for one instrument in [from, to) — the
// equivalent of `tinvest instruments coupons`. The identifier is resolved to
// its instrument_uid first.
func (c *Client) Coupons(ctx context.Context, id string, from, to time.Time) ([]*investapi.Coupon, error) {
	uid, err := c.resolveUID(ctx, id)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.instruments.Coupons(ctx, uid, from, to)
	return list, apiErr(err, info)
}

// AccruedInterests lists accrued-interest values for one bond in [from, to) —
// the equivalent of `tinvest instruments accrued-interest`. The identifier is
// resolved to its instrument_uid first.
func (c *Client) AccruedInterests(ctx context.Context, id string, from, to time.Time) ([]*investapi.AccruedInterest, error) {
	uid, err := c.resolveUID(ctx, id)
	if err != nil {
		return nil, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.instruments.AccruedInterests(ctx, uid, from, to)
	return list, apiErr(err, info)
}

// TradingSchedules returns exchange trading calendars in [from, to) — the
// equivalent of `tinvest instruments schedules`. An empty exchange returns all
// exchanges.
func (c *Client) TradingSchedules(ctx context.Context, exchange string, from, to time.Time) ([]*investapi.TradingSchedule, error) {
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.instruments.Schedules(ctx, exchange, from, to)
	return list, apiErr(err, info)
}
