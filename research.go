package tinvest

import (
	"context"

	brokerresearch "github.com/Dronnn/tinvest/internal/broker/research"
	"github.com/Dronnn/tinvest/internal/transport"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// NewsParams carries the optional pagination fields for News.
type NewsParams struct {
	Cursor *int64
	Limit  *int32
}

// NewsResult is one News page.
type NewsResult struct {
	Items      []*investapi.NewsItem
	HasNext    bool
	NextCursor *int64
}

// ForecastResult holds investment-house targets and the broker consensus.
type ForecastResult struct {
	Targets   []*investapi.GetForecastResponse_TargetItem
	Consensus *investapi.GetForecastResponse_ConsensusItem
}

// ConsensusParams carries the zero-based page settings for Consensus.
type ConsensusParams struct {
	Limit      int32
	PageNumber int32
}

// ConsensusResult is one Consensus page.
type ConsensusResult struct {
	Items []*investapi.GetConsensusForecastsResponse_ConsensusForecastsItem
	Page  *investapi.PageResponse
}

// InsiderDealsParams carries the instrument and page settings for InsiderDeals.
type InsiderDealsParams struct {
	InstrumentID string
	Limit        int32
	Cursor       string
}

// InsiderDealsResult is one InsiderDeals page.
type InsiderDealsResult struct {
	Deals      []*investapi.GetInsiderDealsResponse_InsiderDeal
	NextCursor *string
}

// News fetches exactly one page of current news — the equivalent of
// `tinvest research news`.
func (c *Client) News(ctx context.Context, params NewsParams) (NewsResult, error) {
	ctx, info := transport.WithCallInfo(ctx)
	res, err := c.research.News(ctx, brokerresearch.NewsParams{Cursor: params.Cursor, Limit: params.Limit})
	if err != nil {
		return NewsResult{}, apiErr(err, info)
	}
	return NewsResult{Items: res.Items, HasNext: res.HasNext, NextCursor: res.NextCursor}, nil
}

// Fundamentals returns fundamental statistics for one through 100 asset UIDs —
// the equivalent of `tinvest research fundamentals --asset`. An asset UID for
// an instrument can be obtained from Resolve(...).GetAssetUid().
func (c *Client) Fundamentals(ctx context.Context, assetUIDs ...string) ([]*investapi.GetAssetFundamentalsResponse_StatisticResponse, error) {
	ctx, info := transport.WithCallInfo(ctx)
	list, err := c.research.Fundamentals(ctx, assetUIDs)
	return list, apiErr(err, info)
}

// Forecast returns investment-house forecasts for one instrument — the
// equivalent of `tinvest research forecast`. The identifier is resolved to its
// instrument_uid first.
func (c *Client) Forecast(ctx context.Context, id string) (ForecastResult, error) {
	uid, err := c.resolveUID(ctx, id)
	if err != nil {
		return ForecastResult{}, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	res, err := c.research.Forecast(ctx, uid)
	if err != nil {
		return ForecastResult{}, apiErr(err, info)
	}
	return ForecastResult{Targets: res.Targets, Consensus: res.Consensus}, nil
}

// Consensus fetches exactly one page of consensus forecasts — the equivalent
// of `tinvest research consensus`.
func (c *Client) Consensus(ctx context.Context, params ConsensusParams) (ConsensusResult, error) {
	ctx, info := transport.WithCallInfo(ctx)
	res, err := c.research.Consensus(ctx, brokerresearch.ConsensusParams{Limit: params.Limit, PageNumber: params.PageNumber})
	if err != nil {
		return ConsensusResult{}, apiErr(err, info)
	}
	return ConsensusResult{Items: res.Items, Page: res.Page}, nil
}

// InsiderDeals fetches exactly one page of insider deals for one instrument —
// the equivalent of `tinvest research insider-deals`. params.InstrumentID
// accepts a uid, FIGI, or TICKER@CLASSCODE and is resolved to its
// instrument_uid first.
func (c *Client) InsiderDeals(ctx context.Context, params InsiderDealsParams) (InsiderDealsResult, error) {
	uid, err := c.resolveUID(ctx, params.InstrumentID)
	if err != nil {
		return InsiderDealsResult{}, err
	}
	ctx, info := transport.WithCallInfo(ctx)
	res, err := c.research.InsiderDeals(ctx, brokerresearch.InsiderDealsParams{
		InstrumentID: uid,
		Limit:        params.Limit,
		Cursor:       params.Cursor,
	})
	if err != nil {
		return InsiderDealsResult{}, apiErr(err, info)
	}
	return InsiderDealsResult{Deals: res.Deals, NextCursor: res.NextCursor}, nil
}
