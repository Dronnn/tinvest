// Package instruments resolves instrument identifiers
// (<instrument_uid | FIGI | TICKER@CLASSCODE>) to the full instrument record
// via InstrumentsService, with a local TTL cache, plus free-text search
// (plan §5/§8).
package instruments

import (
	"context"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
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
