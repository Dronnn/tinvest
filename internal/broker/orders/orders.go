// Package orders wraps OrdersService (plan §5/§8/§9): typed placement,
// cancellation, replacement, state lookups, and the two pre-trade checks
// (GetOrderPrice, GetMaxLots). Instrument resolution happens one layer up, in
// internal/broker/instruments; this package takes a resolved instrument_uid.
// It carries no CLI concerns — no cobra, no rendering — so the same surface is
// reusable as a library. The write-ahead ledger and retry marking live in the
// command layer, which sequences Begin -> SendStarted -> Place per §9.
package orders

import (
	"context"

	"google.golang.org/grpc"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over OrdersService.
type Client struct {
	api investapi.OrdersServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewOrdersServiceClient(cc)}
}

// PlaceParams is the fully-resolved description of an order to place. InstrumentID
// is an already-resolved instrument_uid; OrderID is the client idempotency key
// (plan §9), which MUST have been persisted to the intent ledger before Place
// is called.
type PlaceParams struct {
	AccountID    string
	InstrumentID string
	OrderID      string
	Direction    investapi.OrderDirection
	OrderType    investapi.OrderType
	Lots         int64
	Price        *investapi.Quotation // nil for market/bestprice
	TimeInForce  investapi.TimeInForceType
}

// Place posts a synchronous order (PostOrder). The caller is responsible for
// marking ctx idempotent (retry.Idempotent) — safe because OrderID dedups the
// retry server-side — and for the surrounding ledger stages.
func (c Client) Place(ctx context.Context, p PlaceParams) (*investapi.PostOrderResponse, error) {
	return c.api.PostOrder(ctx, &investapi.PostOrderRequest{
		AccountId:    p.AccountID,
		InstrumentId: p.InstrumentID,
		OrderId:      p.OrderID,
		Direction:    p.Direction,
		OrderType:    p.OrderType,
		Quantity:     p.Lots,
		Price:        p.Price,
		TimeInForce:  p.TimeInForce,
	})
}

// PlaceAsync posts an order via PostOrderAsync (plan §8/§9): same idempotency
// contract, but the result carries a trade_intent_id instead of a full order
// state, for high-rate flows.
func (c Client) PlaceAsync(ctx context.Context, p PlaceParams) (*investapi.PostOrderAsyncResponse, error) {
	req := &investapi.PostOrderAsyncRequest{
		AccountId:    p.AccountID,
		InstrumentId: p.InstrumentID,
		OrderId:      p.OrderID,
		Direction:    p.Direction,
		OrderType:    p.OrderType,
		Quantity:     p.Lots,
		Price:        p.Price,
	}
	if p.TimeInForce != investapi.TimeInForceType_TIME_IN_FORCE_UNSPECIFIED {
		tif := p.TimeInForce
		req.TimeInForce = &tif
	}
	return c.api.PostOrderAsync(ctx, req)
}

// Get returns the state of one order. When byRequestID is true the id is
// interpreted as the client idempotency key (order_id passed at placement),
// which is how reconciliation looks up an order it may have created (plan §9);
// otherwise the exchange order id is used.
func (c Client) Get(ctx context.Context, accountID, orderID string, byRequestID bool) (*investapi.OrderState, error) {
	req := &investapi.GetOrderStateRequest{AccountId: accountID, OrderId: orderID}
	if byRequestID {
		t := investapi.OrderIdType_ORDER_ID_TYPE_REQUEST
		req.OrderIdType = &t
	}
	return c.api.GetOrderState(ctx, req)
}

// List returns every active order on the account (GetOrders).
func (c Client) List(ctx context.Context, accountID string) ([]*investapi.OrderState, error) {
	resp, err := c.api.GetOrders(ctx, &investapi.GetOrdersRequest{AccountId: accountID})
	if err != nil {
		return nil, err
	}
	return resp.GetOrders(), nil
}

// Cancel cancels one order by its exchange order id (CancelOrder). Cancellation
// is convergent when repeated, so the caller may mark ctx idempotent for retry.
func (c Client) Cancel(ctx context.Context, accountID, orderID string) (*investapi.CancelOrderResponse, error) {
	return c.api.CancelOrder(ctx, &investapi.CancelOrderRequest{AccountId: accountID, OrderId: orderID})
}

// ReplaceParams describes an order replacement (ReplaceOrder). IdempotencyKey is
// a fresh client key that overwrites the original order's key.
type ReplaceParams struct {
	AccountID      string
	OrderID        string // exchange order id being replaced
	IdempotencyKey string
	Lots           int64
	Price          *investapi.Quotation
}

// Replace cancels and re-creates an order atomically (ReplaceOrder). It carries
// its own idempotency key; the broker's retention guarantees for it are the
// same as PostOrder.
func (c Client) Replace(ctx context.Context, p ReplaceParams) (*investapi.PostOrderResponse, error) {
	return c.api.ReplaceOrder(ctx, &investapi.ReplaceOrderRequest{
		AccountId:      p.AccountID,
		OrderId:        p.OrderID,
		IdempotencyKey: p.IdempotencyKey,
		Quantity:       p.Lots,
		Price:          p.Price,
	})
}

// PreviewParams describes a GetOrderPrice pre-trade cost check.
type PreviewParams struct {
	AccountID    string
	InstrumentID string
	Direction    investapi.OrderDirection
	Lots         int64
	Price        *investapi.Quotation
}

// Preview returns the projected cost and commission of an order without placing
// it (GetOrderPrice), used by --dry-run and `orders preview`.
func (c Client) Preview(ctx context.Context, p PreviewParams) (*investapi.GetOrderPriceResponse, error) {
	return c.api.GetOrderPrice(ctx, &investapi.GetOrderPriceRequest{
		AccountId:    p.AccountID,
		InstrumentId: p.InstrumentID,
		Direction:    p.Direction,
		Quantity:     p.Lots,
		Price:        p.Price,
	})
}

// MaxLots returns the maximum tradable lots for an instrument (GetMaxLots),
// used by --dry-run and `orders max-lots`. Price is optional and refines the
// buy-side limits.
func (c Client) MaxLots(ctx context.Context, accountID, instrumentID string, price *investapi.Quotation) (*investapi.GetMaxLotsResponse, error) {
	return c.api.GetMaxLots(ctx, &investapi.GetMaxLotsRequest{
		AccountId:    accountID,
		InstrumentId: instrumentID,
		Price:        price,
	})
}
