// Package stoporders wraps StopOrdersService (plan §1.1/§8/§9): typed
// placement, cancellation, and listing of stop-loss/take-profit/stop-limit
// orders, including trailing take-profit. Instrument resolution happens one
// layer up, in internal/broker/instruments; this package takes a resolved
// instrument_uid. It carries no CLI concerns — no cobra, no rendering — so the
// same surface is reusable as a library.
//
// Idempotency note (plan §1.1/§9, corrected 2026-07-18): the current contract
// has a REQUIRED order_id field on PostStopOrder, but its retention/dedup
// guarantees are undocumented. Unlike internal/broker/orders, this package
// and its callers must NEVER mark a placement context idempotent for retry —
// the write-ahead ledger and exit-7 unknown-state protocol are the recovery
// path instead (see cmd/tinvest/stoporders.go).
package stoporders

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	investapi "tinvest/internal/pb/investapi"
)

// Client is a thin typed wrapper over StopOrdersService.
type Client struct {
	api investapi.StopOrdersServiceClient
}

// New builds a client on top of an established connection.
func New(cc grpc.ClientConnInterface) Client {
	return Client{api: investapi.NewStopOrdersServiceClient(cc)}
}

// TrailingParams describes a trailing take-profit's indent/spread (plan §8):
// valid only when StopOrderType is take-profit and TakeProfitType is
// trailing.
type TrailingParams struct {
	Indent     *investapi.Quotation
	IndentType investapi.TrailingValueType
	Spread     *investapi.Quotation
	SpreadType investapi.TrailingValueType
}

// PlaceParams is the fully-resolved description of a stop order to place.
// InstrumentID is an already-resolved instrument_uid; OrderID is the client
// idempotency key (plan §1.1/§9), which MUST have been persisted to the
// intent ledger before Place is called.
type PlaceParams struct {
	AccountID         string
	InstrumentID      string
	OrderID           string
	Direction         investapi.StopOrderDirection
	StopOrderType     investapi.StopOrderType
	Quantity          int64
	Price             *investapi.Quotation // required for stop-limit only
	StopPrice         *investapi.Quotation // always required
	ExpirationType    investapi.StopOrderExpirationType
	ExpireDate        *timestamppb.Timestamp // required for GTD
	ExchangeOrderType investapi.ExchangeOrderType
	TakeProfitType    investapi.TakeProfitType
	Trailing          *TrailingParams // take-profit only
}

// Place posts a stop order (PostStopOrder). The caller MUST NOT mark ctx
// idempotent: per plan §1.1/§9 the retention guarantees of the required
// order_id field are undocumented, so auto-retry stays off until
// capability-tested. An ambiguous outcome is the command layer's job to
// surface as exit 7 with a reconcile hint, not to retry here.
func (c Client) Place(ctx context.Context, p PlaceParams) (*investapi.PostStopOrderResponse, error) {
	req := &investapi.PostStopOrderRequest{
		InstrumentId:      p.InstrumentID,
		AccountId:         p.AccountID,
		OrderId:           p.OrderID,
		Direction:         p.Direction,
		StopOrderType:     p.StopOrderType,
		Quantity:          p.Quantity,
		Price:             p.Price,
		StopPrice:         p.StopPrice,
		ExpirationType:    p.ExpirationType,
		ExpireDate:        p.ExpireDate,
		ExchangeOrderType: p.ExchangeOrderType,
		TakeProfitType:    p.TakeProfitType,
	}
	if p.Trailing != nil {
		req.TrailingData = &investapi.PostStopOrderRequest_TrailingData{
			Indent:     p.Trailing.Indent,
			IndentType: p.Trailing.IndentType,
			Spread:     p.Trailing.Spread,
			SpreadType: p.Trailing.SpreadType,
		}
	}
	return c.api.PostStopOrder(ctx, req)
}

// ListParams filters GetStopOrders. Zero values mean "no filter" (status
// unspecified lists everything the broker returns for the account; From/To
// nil omit the time bounds).
type ListParams struct {
	AccountID string
	Status    investapi.StopOrderStatusOption
	From, To  *timestamppb.Timestamp
}

// List returns stop orders on the account (GetStopOrders).
func (c Client) List(ctx context.Context, p ListParams) ([]*investapi.StopOrder, error) {
	resp, err := c.api.GetStopOrders(ctx, &investapi.GetStopOrdersRequest{
		AccountId: p.AccountID,
		Status:    p.Status,
		From:      p.From,
		To:        p.To,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetStopOrders(), nil
}

// Cancel cancels one stop order by its exchange-assigned stop_order_id
// (CancelStopOrder). Cancellation is convergent when repeated (cancelling an
// already-cancelled or unknown stop order fails safely), so the caller may
// mark ctx idempotent for retry — this differs from Place, which must not be
// retried automatically (plan §9).
func (c Client) Cancel(ctx context.Context, accountID, stopOrderID string) (*investapi.CancelStopOrderResponse, error) {
	return c.api.CancelStopOrder(ctx, &investapi.CancelStopOrderRequest{
		AccountId:   accountID,
		StopOrderId: stopOrderID,
	})
}
