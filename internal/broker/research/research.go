// Package research wraps the read-only research RPCs on InstrumentsService.
package research

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// Client is a thin typed wrapper over the InstrumentsService research surface.
type Client struct {
	api investapi.InstrumentsServiceClient
}

// New builds a research client on top of the caller's established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewInstrumentsServiceClient(cc)}
}

// NewsParams contains the native optional pagination fields for News.
type NewsParams struct {
	Cursor *int64
	Limit  *int32
}

// NewsResult is one News RPC page.
type NewsResult struct {
	Items      []*investapi.NewsItem
	HasNext    bool
	NextCursor *int64
}

// News fetches exactly one page of current news.
func (c Client) News(ctx context.Context, params NewsParams) (NewsResult, error) {
	if params.Limit != nil && *params.Limit <= 0 {
		return NewsResult{}, fmt.Errorf("invalid news limit %d: want a positive value", *params.Limit)
	}
	response, err := c.api.News(ctx, &investapi.NewsRequest{Cursor: params.Cursor, Limit: params.Limit})
	if err != nil {
		return NewsResult{}, err
	}
	return NewsResult{
		Items: response.GetItems(), HasNext: response.GetHasNext(), NextCursor: response.NextCursor,
	}, nil
}

// Fundamentals returns fundamental statistics for one through 100 asset UIDs.
func (c Client) Fundamentals(ctx context.Context, assetUIDs []string) ([]*investapi.GetAssetFundamentalsResponse_StatisticResponse, error) {
	if err := validateAssets(assetUIDs); err != nil {
		return nil, err
	}
	response, err := c.api.GetAssetFundamentals(ctx, &investapi.GetAssetFundamentalsRequest{Assets: assetUIDs})
	if err != nil {
		return nil, err
	}
	return response.GetFundamentals(), nil
}

func validateAssets(assetUIDs []string) error {
	if len(assetUIDs) == 0 || len(assetUIDs) > 100 {
		return fmt.Errorf("invalid fundamentals asset count %d: want 1 through 100", len(assetUIDs))
	}
	for _, assetUID := range assetUIDs {
		if strings.TrimSpace(assetUID) == "" {
			return fmt.Errorf("fundamentals asset UID must not be empty")
		}
	}
	return nil
}

// ForecastResult contains the investment-house targets and broker consensus.
type ForecastResult struct {
	Targets   []*investapi.GetForecastResponse_TargetItem
	Consensus *investapi.GetForecastResponse_ConsensusItem
}

// Forecast fetches investment-house forecasts for one resolved instrument UID.
func (c Client) Forecast(ctx context.Context, instrumentUID string) (ForecastResult, error) {
	if strings.TrimSpace(instrumentUID) == "" {
		return ForecastResult{}, fmt.Errorf("forecast instrument UID must not be empty")
	}
	response, err := c.api.GetForecastBy(ctx, &investapi.GetForecastRequest{InstrumentId: instrumentUID})
	if err != nil {
		return ForecastResult{}, err
	}
	return ForecastResult{Targets: response.GetTargets(), Consensus: response.GetConsensus()}, nil
}

// ConsensusParams contains the native zero-based page settings.
type ConsensusParams struct {
	Limit      int32
	PageNumber int32
}

// ConsensusResult is one GetConsensusForecasts RPC page.
type ConsensusResult struct {
	Items []*investapi.GetConsensusForecastsResponse_ConsensusForecastsItem
	Page  *investapi.PageResponse
}

// Consensus fetches exactly one page of consensus forecasts.
func (c Client) Consensus(ctx context.Context, params ConsensusParams) (ConsensusResult, error) {
	if params.Limit <= 0 {
		return ConsensusResult{}, fmt.Errorf("invalid consensus limit %d: want a positive value", params.Limit)
	}
	if params.PageNumber < 0 {
		return ConsensusResult{}, fmt.Errorf("invalid consensus page number %d: want zero or greater", params.PageNumber)
	}
	response, err := c.api.GetConsensusForecasts(ctx, &investapi.GetConsensusForecastsRequest{
		Paging: &investapi.Page{Limit: params.Limit, PageNumber: params.PageNumber},
	})
	if err != nil {
		return ConsensusResult{}, err
	}
	return ConsensusResult{Items: response.GetItems(), Page: response.GetPage()}, nil
}

// InsiderDealsParams contains the instrument and native page settings.
type InsiderDealsParams struct {
	InstrumentID string
	Limit        int32
	Cursor       string
}

// InsiderDealsResult is one GetInsiderDeals RPC page.
type InsiderDealsResult struct {
	Deals      []*investapi.GetInsiderDealsResponse_InsiderDeal
	NextCursor *string
}

// InsiderDeals fetches exactly one page of insider transactions.
func (c Client) InsiderDeals(ctx context.Context, params InsiderDealsParams) (InsiderDealsResult, error) {
	if strings.TrimSpace(params.InstrumentID) == "" {
		return InsiderDealsResult{}, fmt.Errorf("insider-deals instrument UID must not be empty")
	}
	if params.Limit <= 0 || params.Limit > 100 {
		return InsiderDealsResult{}, fmt.Errorf("invalid insider-deals limit %d: want 1 through 100", params.Limit)
	}
	request := &investapi.GetInsiderDealsRequest{InstrumentId: params.InstrumentID, Limit: params.Limit}
	if params.Cursor != "" {
		request.NextCursor = &params.Cursor
	}
	response, err := c.api.GetInsiderDeals(ctx, request)
	if err != nil {
		return InsiderDealsResult{}, err
	}
	return InsiderDealsResult{Deals: response.GetInsiderDeals(), NextCursor: response.NextCursor}, nil
}
