package render

import (
	"io"
	"strconv"

	investapi "tinvest/internal/pb/investapi"
)

// LotsView reports requested/executed/remaining lots separately (plan §9): a
// partial fill is honest about what is done and what is left.
type LotsView struct {
	Requested int64 `json:"requested"`
	Executed  int64 `json:"executed"`
	Remaining int64 `json:"remaining"`
}

func lotsView(requested, executed int64) LotsView {
	remaining := requested - executed
	if remaining < 0 {
		remaining = 0
	}
	return LotsView{Requested: requested, Executed: executed, Remaining: remaining}
}

// PlaceResultView is the JSON shape of a placed order's outcome. OrderID is the
// exchange id; ClientOrderID is the idempotency key the CLI used (the durable
// intent key, plan §9).
type PlaceResultView struct {
	OrderID       string   `json:"order_id"`
	ClientOrderID string   `json:"client_order_id"`
	Lifecycle     string   `json:"lifecycle"`
	Direction     string   `json:"direction"`
	OrderType     string   `json:"order_type"`
	Lots          LotsView `json:"lots"`
	InstrumentUID string   `json:"instrument_uid,omitempty"`
	Ticker        string   `json:"ticker,omitempty"`
	InitialPrice  *Decimal `json:"initial_order_price,omitempty"`
	ExecutedPrice *Decimal `json:"executed_order_price,omitempty"`
	TotalAmount   *Decimal `json:"total_order_amount,omitempty"`
	Commission    *Decimal `json:"initial_commission,omitempty"`
	Message       string   `json:"message,omitempty"`
}

// PlaceResult converts a PostOrderResponse. clientOrderID is threaded through
// because the response's order_request_id is not always echoed.
func PlaceResult(r *investapi.PostOrderResponse, clientOrderID string, lifecycle string) PlaceResultView {
	v := PlaceResultView{
		OrderID:       r.GetOrderId(),
		ClientOrderID: firstNonEmptyStr(clientOrderID, r.GetOrderRequestId()),
		Lifecycle:     lifecycle,
		Direction:     r.GetDirection().String(),
		OrderType:     r.GetOrderType().String(),
		Lots:          lotsView(r.GetLotsRequested(), r.GetLotsExecuted()),
		InstrumentUID: r.GetInstrumentUid(),
		Ticker:        r.GetTicker(),
		InitialPrice:  moneyPtr(r.GetInitialOrderPrice()),
		ExecutedPrice: moneyPtr(r.GetExecutedOrderPrice()),
		TotalAmount:   moneyPtr(r.GetTotalOrderAmount()),
		Commission:    moneyPtr(r.GetInitialCommission()),
		Message:       r.GetMessage(),
	}
	return v
}

// AsyncResultView is the JSON shape of a PostOrderAsync outcome, carrying the
// trade_intent_id instead of a full order state (plan §8).
type AsyncResultView struct {
	ClientOrderID string `json:"client_order_id"`
	TradeIntentID string `json:"trade_intent_id,omitempty"`
	Lifecycle     string `json:"lifecycle"`
}

// AsyncResult converts a PostOrderAsyncResponse.
func AsyncResult(r *investapi.PostOrderAsyncResponse, clientOrderID, lifecycle string) AsyncResultView {
	return AsyncResultView{
		ClientOrderID: firstNonEmptyStr(clientOrderID, r.GetOrderRequestId()),
		TradeIntentID: r.GetTradeIntentId(),
		Lifecycle:     lifecycle,
	}
}

// OrderStateView is the JSON shape of one order's full state (plan §9).
type OrderStateView struct {
	OrderID       string   `json:"order_id"`
	ClientOrderID string   `json:"client_order_id,omitempty"`
	Lifecycle     string   `json:"lifecycle"`
	Direction     string   `json:"direction"`
	OrderType     string   `json:"order_type"`
	Lots          LotsView `json:"lots"`
	InstrumentUID string   `json:"instrument_uid,omitempty"`
	Ticker        string   `json:"ticker,omitempty"`
	Currency      string   `json:"currency,omitempty"`
	InitialPrice  *Decimal `json:"initial_order_price,omitempty"`
	ExecutedPrice *Decimal `json:"executed_order_price,omitempty"`
	TotalAmount   *Decimal `json:"total_order_amount,omitempty"`
	Commission    *Decimal `json:"executed_commission,omitempty"`
	OrderDate     string   `json:"order_date,omitempty"`
}

// OrderState converts an OrderState. lifecycle is passed in so callers use the
// shared status mapping (internal/broker/orders.Lifecycle) rather than
// duplicating the enum switch here.
func OrderState(s *investapi.OrderState, lifecycle string) OrderStateView {
	return OrderStateView{
		OrderID:       s.GetOrderId(),
		ClientOrderID: s.GetOrderRequestId(),
		Lifecycle:     lifecycle,
		Direction:     s.GetDirection().String(),
		OrderType:     s.GetOrderType().String(),
		Lots:          lotsView(s.GetLotsRequested(), s.GetLotsExecuted()),
		InstrumentUID: s.GetInstrumentUid(),
		Ticker:        s.GetTicker(),
		Currency:      s.GetCurrency(),
		InitialPrice:  moneyPtr(s.GetInitialOrderPrice()),
		ExecutedPrice: moneyPtr(s.GetExecutedOrderPrice()),
		TotalAmount:   moneyPtr(s.GetTotalOrderAmount()),
		Commission:    moneyPtr(s.GetExecutedCommission()),
		OrderDate:     Timestamp(s.GetOrderDate()),
	}
}

// OrderStatesTable renders an order list for humans.
func OrderStatesTable(w io.Writer, views []OrderStateView) error {
	rows := make([][]string, 0, len(views))
	for _, v := range views {
		rows = append(rows, []string{v.OrderID, v.Lifecycle, v.Direction, v.Ticker,
			itoa(v.Lots.Requested), itoa(v.Lots.Executed)})
	}
	return Table(w, []string{"ORDER_ID", "LIFECYCLE", "DIRECTION", "TICKER", "REQUESTED", "EXECUTED"}, rows)
}

// PreviewView is the JSON shape of a GetOrderPrice pre-trade check.
type PreviewView struct {
	LotsRequested int64    `json:"lots_requested"`
	InitialAmount *Decimal `json:"initial_order_amount,omitempty"`
	TotalAmount   *Decimal `json:"total_order_amount,omitempty"`
	Commission    *Decimal `json:"executed_commission,omitempty"`
	CommissionRub *Decimal `json:"executed_commission_rub,omitempty"`
}

// Preview converts a GetOrderPriceResponse.
func Preview(r *investapi.GetOrderPriceResponse) PreviewView {
	return PreviewView{
		LotsRequested: r.GetLotsRequested(),
		InitialAmount: moneyPtr(r.GetInitialOrderAmount()),
		TotalAmount:   moneyPtr(r.GetTotalOrderAmount()),
		Commission:    moneyPtr(r.GetExecutedCommission()),
		CommissionRub: moneyPtr(r.GetExecutedCommissionRub()),
	}
}

// MaxLotsView is the JSON shape of a GetMaxLots pre-trade check.
type MaxLotsView struct {
	Currency        string `json:"currency,omitempty"`
	BuyMaxLots      int64  `json:"buy_max_lots"`
	BuyMaxMarketLot int64  `json:"buy_max_market_lots"`
	SellMaxLots     int64  `json:"sell_max_lots"`
}

// MaxLots converts a GetMaxLotsResponse.
func MaxLots(r *investapi.GetMaxLotsResponse) MaxLotsView {
	return MaxLotsView{
		Currency:        r.GetCurrency(),
		BuyMaxLots:      r.GetBuyLimits().GetBuyMaxLots(),
		BuyMaxMarketLot: r.GetBuyLimits().GetBuyMaxMarketLots(),
		SellMaxLots:     r.GetSellLimits().GetSellMaxLots(),
	}
}

// ReconcileOutcomeView is one resolved intent from `orders reconcile` (plan §9).
type ReconcileOutcomeView struct {
	IntentID      string `json:"intent_id"`
	ClientOrderID string `json:"client_order_id,omitempty"`
	AccountID     string `json:"account_id,omitempty"`
	Outcome       string `json:"outcome"`
	OrderID       string `json:"order_id,omitempty"`
	Lifecycle     string `json:"lifecycle,omitempty"`
	Error         string `json:"error,omitempty"`
}

// ReconcileTable renders reconcile outcomes for humans.
func ReconcileTable(w io.Writer, views []ReconcileOutcomeView) error {
	rows := make([][]string, 0, len(views))
	for _, v := range views {
		rows = append(rows, []string{v.IntentID, v.ClientOrderID, v.Outcome, v.OrderID, v.Lifecycle})
	}
	return Table(w, []string{"INTENT_ID", "CLIENT_ORDER_ID", "OUTCOME", "ORDER_ID", "LIFECYCLE"}, rows)
}

func moneyPtr(m *investapi.MoneyValue) *Decimal {
	if m == nil {
		return nil
	}
	d := Money(m)
	return &d
}

func firstNonEmptyStr(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
