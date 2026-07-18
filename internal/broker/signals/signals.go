// Package signals wraps analyst and technical signal reads.
package signals

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

const signalsPageLimit int32 = 100

// Client is a thin typed wrapper over SignalService.
type Client struct {
	api investapi.SignalServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewSignalServiceClient(cc)}
}

// Strategies returns all signal strategies.
func (c Client) Strategies(ctx context.Context) ([]*investapi.Strategy, error) {
	response, err := c.api.GetStrategies(ctx, &investapi.GetStrategiesRequest{})
	if err != nil {
		return nil, err
	}
	return response.GetStrategies(), nil
}

// Signals returns all signal pages, optionally filtered by strategy id.
func (c Client) Signals(ctx context.Context, strategyID string) ([]*investapi.Signal, error) {
	result := make([]*investapi.Signal, 0)
	for pageNumber := int32(0); ; pageNumber++ {
		request := &investapi.GetSignalsRequest{
			Paging: &investapi.Page{Limit: signalsPageLimit, PageNumber: pageNumber},
		}
		if strategyID != "" {
			request.StrategyId = &strategyID
		}
		response, err := c.api.GetSignals(ctx, request)
		if err != nil {
			return nil, err
		}
		page := response.GetSignals()
		result = append(result, page...)
		paging := response.GetPaging()
		if paging == nil || int32(len(result)) >= paging.GetTotalCount() {
			return result, nil
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("signals pagination stopped before total_count %d", paging.GetTotalCount())
		}
	}
}
