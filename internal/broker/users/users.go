// Package users wraps the user-info surface of UsersService.
package users

import (
	"context"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over the user calls.
type Client struct {
	api investapi.UsersServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewUsersServiceClient(cc)}
}

// Info returns the token owner's profile information.
func (c Client) Info(ctx context.Context) (*investapi.GetInfoResponse, error) {
	return c.api.GetInfo(ctx, &investapi.GetInfoRequest{})
}
