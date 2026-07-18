// Package sandbox wraps the account-management surface of SandboxService
// (plan §1.1/§8): opening/closing sandbox accounts, listing them, and paying
// virtual money in. It deliberately does not wrap the sandbox order/stop-order
// mirror RPCs (PostSandboxOrder etc.) — those are exercised through the
// regular orders/stoporders packages against the sandbox endpoint, which the
// command layer forces (cmd/tinvest/sandbox.go), keeping one placement code
// path instead of two. It carries no CLI concerns — no cobra, no rendering —
// so the same surface is reusable as a library.
package sandbox

import (
	"context"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over the account-management calls of
// SandboxService.
type Client struct {
	api investapi.SandboxServiceClient
}

// New builds a client on top of an established connection. The caller is
// responsible for ensuring that connection targets the sandbox endpoint
// (config.SandboxEndpoint) — this package does not check.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewSandboxServiceClient(cc)}
}

// Open creates a new sandbox account (OpenSandboxAccount). An empty name
// leaves the broker's default naming in place.
func (c Client) Open(ctx context.Context, name string) (*investapi.OpenSandboxAccountResponse, error) {
	req := &investapi.OpenSandboxAccountRequest{}
	if name != "" {
		req.Name = &name
	}
	return c.api.OpenSandboxAccount(ctx, req)
}

// Close closes a sandbox account (CloseSandboxAccount).
func (c Client) Close(ctx context.Context, accountID string) (*investapi.CloseSandboxAccountResponse, error) {
	return c.api.CloseSandboxAccount(ctx, &investapi.CloseSandboxAccountRequest{AccountId: accountID})
}

// Accounts lists sandbox accounts visible to the token (GetSandboxAccounts),
// reusing the same Account type as the production accounts surface.
func (c Client) Accounts(ctx context.Context) ([]*investapi.Account, error) {
	resp, err := c.api.GetSandboxAccounts(ctx, &investapi.GetAccountsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetAccounts(), nil
}

// PayIn credits virtual money to a sandbox account (SandboxPayIn). amount is
// a fully-formed MoneyValue (units+nano+currency); the CLI layer converts the
// decimal --amount flag before calling this.
func (c Client) PayIn(ctx context.Context, accountID string, amount *investapi.MoneyValue) (*investapi.SandboxPayInResponse, error) {
	return c.api.SandboxPayIn(ctx, &investapi.SandboxPayInRequest{AccountId: accountID, Amount: amount})
}
