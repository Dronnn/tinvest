// Package instruments resolves instrument identifiers
// (<instrument_uid | FIGI | TICKER@CLASSCODE>) to the full instrument record
// via InstrumentsService, with a local TTL cache, plus free-text search
// (plan §5/§8).
package instruments

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// Client is a thin typed wrapper over the instrument-resolution surface of
// InstrumentsService.
type Client struct {
	api   investapi.InstrumentsServiceClient
	cache *Cache // nil disables caching
}

// New builds a client on top of an established connection. cache may be nil,
// which disables caching for this client.
func New(cc grpc.ClientConnInterface, cache *Cache) Client {
	return Client{api: investapi.NewInstrumentsServiceClient(cc), cache: cache}
}

// Resolve normalizes raw (an instrument_uid, FIGI, or TICKER@CLASSCODE) to
// the full instrument record via GetInstrumentBy. Malformed input never
// reaches the network: Classify runs first and returns an *InvalidIDError
// instead. Successful resolutions are cached under the raw input string;
// noCache bypasses both the cache read and the write. Broker errors are
// never cached.
func (c Client) Resolve(ctx context.Context, raw string, noCache bool) (*investapi.Instrument, error) {
	parsed, err := Classify(raw)
	if err != nil {
		return nil, err
	}

	if !noCache && c.cache != nil {
		if inst, ok := c.cache.Get(raw); ok {
			return inst, nil
		}
	}

	req := &investapi.InstrumentRequest{Id: parsed.Raw}
	switch parsed.Kind {
	case KindUID:
		req.IdType = investapi.InstrumentIdType_INSTRUMENT_ID_TYPE_UID
	case KindFIGI:
		req.IdType = investapi.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI
	case KindTicker:
		req.IdType = investapi.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER
		req.Id = parsed.Ticker
		classCode := parsed.ClassCode
		req.ClassCode = &classCode
	}

	resp, err := c.api.GetInstrumentBy(ctx, req)
	if err != nil {
		return nil, err
	}
	inst := resp.GetInstrument()

	if !noCache && c.cache != nil {
		// Best-effort: a cache write failure (e.g. read-only filesystem)
		// must not fail a command that already has its answer.
		_ = c.cache.Put(raw, inst)
	}
	return inst, nil
}

// Find searches instruments by free-text query via FindInstrument.
func (c Client) Find(ctx context.Context, query string) ([]*investapi.InstrumentShort, error) {
	resp, err := c.api.FindInstrument(ctx, &investapi.FindInstrumentRequest{Query: query})
	if err != nil {
		return nil, err
	}
	return resp.GetInstruments(), nil
}

// ListedInstrument is the common reference-data projection shared by all six
// list RPCs exposed by the CLI.
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

// ValidateType accepts the six per-type list surfaces exposed by the CLI.
func ValidateType(instrumentType string) error {
	switch instrumentType {
	case "share", "bond", "etf", "currency", "future", "option":
		return nil
	default:
		return fmt.Errorf("invalid instrument type %q: want share, bond, etf, currency, future, or option", instrumentType)
	}
}

// List calls the type-specific list RPC with INSTRUMENT_STATUS_BASE.
func (c Client) List(ctx context.Context, instrumentType string) ([]ListedInstrument, error) {
	if err := ValidateType(instrumentType); err != nil {
		return nil, err
	}
	status := investapi.InstrumentStatus_INSTRUMENT_STATUS_BASE
	request := &investapi.InstrumentsRequest{InstrumentStatus: &status}
	switch instrumentType {
	case "share":
		response, err := c.api.Shares(ctx, request)
		if err != nil {
			return nil, err
		}
		result := make([]ListedInstrument, 0, len(response.GetInstruments()))
		for _, instrument := range response.GetInstruments() {
			result = append(result, listedInstrument(instrumentType, instrument.GetFigi(), instrument))
		}
		return result, nil
	case "bond":
		response, err := c.api.Bonds(ctx, request)
		if err != nil {
			return nil, err
		}
		result := make([]ListedInstrument, 0, len(response.GetInstruments()))
		for _, instrument := range response.GetInstruments() {
			result = append(result, listedInstrument(instrumentType, instrument.GetFigi(), instrument))
		}
		return result, nil
	case "etf":
		response, err := c.api.Etfs(ctx, request)
		if err != nil {
			return nil, err
		}
		result := make([]ListedInstrument, 0, len(response.GetInstruments()))
		for _, instrument := range response.GetInstruments() {
			result = append(result, listedInstrument(instrumentType, instrument.GetFigi(), instrument))
		}
		return result, nil
	case "currency":
		response, err := c.api.Currencies(ctx, request)
		if err != nil {
			return nil, err
		}
		result := make([]ListedInstrument, 0, len(response.GetInstruments()))
		for _, instrument := range response.GetInstruments() {
			result = append(result, listedInstrument(instrumentType, instrument.GetFigi(), instrument))
		}
		return result, nil
	case "future":
		response, err := c.api.Futures(ctx, request)
		if err != nil {
			return nil, err
		}
		result := make([]ListedInstrument, 0, len(response.GetInstruments()))
		for _, instrument := range response.GetInstruments() {
			result = append(result, listedInstrument(instrumentType, instrument.GetFigi(), instrument))
		}
		return result, nil
	case "option":
		// Options is deprecated upstream, but remains the only unfiltered
		// per-type option-list RPC; OptionsBy requires a basic-asset filter.
		response, err := c.api.Options(ctx, request) //nolint:staticcheck
		if err != nil {
			return nil, err
		}
		result := make([]ListedInstrument, 0, len(response.GetInstruments()))
		for _, instrument := range response.GetInstruments() {
			result = append(result, listedInstrument(instrumentType, "", instrument))
		}
		return result, nil
	}
	return nil, fmt.Errorf("unreachable instrument type %q", instrumentType)
}

type commonListedInstrument interface {
	GetUid() string
	GetTicker() string
	GetClassCode() string
	GetName() string
	GetLot() int32
	GetCurrency() string
	GetTradingStatus() investapi.SecurityTradingStatus
}

func listedInstrument(instrumentType, figi string, instrument commonListedInstrument) ListedInstrument {
	return ListedInstrument{
		UID: instrument.GetUid(), FIGI: figi, Ticker: instrument.GetTicker(), ClassCode: instrument.GetClassCode(),
		Name: instrument.GetName(), Type: instrumentType, Lot: instrument.GetLot(), Currency: instrument.GetCurrency(),
		TradingStatus: instrument.GetTradingStatus(),
	}
}

// Dividends returns dividend events for one resolved instrument.
func (c Client) Dividends(ctx context.Context, instrumentID string, from, to time.Time) ([]*investapi.Dividend, error) {
	response, err := c.api.GetDividends(ctx, &investapi.GetDividendsRequest{
		InstrumentId: instrumentID, From: timestamppb.New(from), To: timestamppb.New(to),
	})
	if err != nil {
		return nil, err
	}
	return response.GetDividends(), nil
}

// Coupons returns bond coupon events for one resolved instrument.
func (c Client) Coupons(ctx context.Context, instrumentID string, from, to time.Time) ([]*investapi.Coupon, error) {
	response, err := c.api.GetBondCoupons(ctx, &investapi.GetBondCouponsRequest{
		InstrumentId: instrumentID, From: timestamppb.New(from), To: timestamppb.New(to),
	})
	if err != nil {
		return nil, err
	}
	return response.GetEvents(), nil
}

// AccruedInterests returns accrued-interest values for one resolved bond.
func (c Client) AccruedInterests(ctx context.Context, instrumentID string, from, to time.Time) ([]*investapi.AccruedInterest, error) {
	response, err := c.api.GetAccruedInterests(ctx, &investapi.GetAccruedInterestsRequest{
		InstrumentId: instrumentID, From: timestamppb.New(from), To: timestamppb.New(to),
	})
	if err != nil {
		return nil, err
	}
	return response.GetAccruedInterests(), nil
}

// Schedules returns trading calendars for one exchange or all exchanges when
// exchange is empty.
func (c Client) Schedules(ctx context.Context, exchange string, from, to time.Time) ([]*investapi.TradingSchedule, error) {
	request := &investapi.TradingSchedulesRequest{From: timestamppb.New(from), To: timestamppb.New(to)}
	if exchange != "" {
		request.Exchange = &exchange
	}
	response, err := c.api.TradingSchedules(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.GetExchanges(), nil
}
