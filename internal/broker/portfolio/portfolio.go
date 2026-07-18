// Package portfolio wraps the account portfolio, positions, and withdraw-limit
// reads of OperationsService.
package portfolio

import (
	"context"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over account observation calls.
type Client struct {
	api investapi.OperationsServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewOperationsServiceClient(cc)}
}

// Portfolio returns totals, yield, and portfolio positions for an account.
func (c Client) Portfolio(ctx context.Context, accountID string) (*investapi.PortfolioResponse, error) {
	return c.api.GetPortfolio(ctx, &investapi.PortfolioRequest{AccountId: accountID})
}

// Positions returns money, securities, futures, and options positions.
func (c Client) Positions(ctx context.Context, accountID string) (*investapi.PositionsResponse, error) {
	return c.api.GetPositions(ctx, &investapi.PositionsRequest{AccountId: accountID})
}

// WithdrawLimits returns money currently available and blocked for withdrawal.
func (c Client) WithdrawLimits(ctx context.Context, accountID string) (*investapi.WithdrawLimitsResponse, error) {
	return c.api.GetWithdrawLimits(ctx, &investapi.WithdrawLimitsRequest{AccountId: accountID})
}
