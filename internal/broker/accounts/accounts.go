// Package accounts wraps the accounts surface of UsersService.
package accounts

import (
	"context"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over the accounts calls.
type Client struct {
	api investapi.UsersServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewUsersServiceClient(cc)}
}

// List returns all accounts visible to the token.
func (c Client) List(ctx context.Context) ([]*investapi.Account, error) {
	resp, err := c.api.GetAccounts(ctx, &investapi.GetAccountsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetAccounts(), nil
}
