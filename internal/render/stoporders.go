package render

import (
	"io"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// PlaceStopResultView is the JSON shape of a placed stop order's outcome.
// Unlike a regular order placement, PostStopOrder returns no execution state
// synchronously — only the broker-assigned stop_order_id and an echo of the
// client idempotency key.
type PlaceStopResultView struct {
	StopOrderID   string `json:"stop_order_id"`
	ClientOrderID string `json:"client_order_id"`
}

// PlaceStopResult converts a PostStopOrderResponse. clientOrderID is threaded
// through because the response's order_request_id is not always echoed.
func PlaceStopResult(r *investapi.PostStopOrderResponse, clientOrderID string) PlaceStopResultView {
	return PlaceStopResultView{
		StopOrderID:   r.GetStopOrderId(),
		ClientOrderID: firstNonEmptyStr(clientOrderID, r.GetOrderRequestId()),
	}
}

// TrailingView is the JSON shape of a trailing take-profit's parameters and
// live state.
type TrailingView struct {
	Indent     *Decimal `json:"indent,omitempty"`
	IndentType string   `json:"indent_type,omitempty"`
	Spread     *Decimal `json:"spread,omitempty"`
	SpreadType string   `json:"spread_type,omitempty"`
	Status     string   `json:"status,omitempty"`
	Price      *Decimal `json:"price,omitempty"`
	Extremum   *Decimal `json:"extremum,omitempty"`
}

func trailingView(t *investapi.StopOrder_TrailingData) *TrailingView {
	if t == nil {
		return nil
	}
	return &TrailingView{
		Indent:     quotationPtr(t.GetIndent()),
		IndentType: t.GetIndentType().String(),
		Spread:     quotationPtr(t.GetSpread()),
		SpreadType: t.GetSpreadType().String(),
		Status:     t.GetStatus().String(),
		Price:      quotationPtr(t.GetPrice()),
		Extremum:   quotationPtr(t.GetExtr()),
	}
}

// StopOrderView is the JSON shape of one stop order's full state.
type StopOrderView struct {
	StopOrderID        string        `json:"stop_order_id"`
	Status             string        `json:"status"`
	Direction          string        `json:"direction"`
	StopOrderType      string        `json:"stop_order_type"`
	ExchangeOrderType  string        `json:"exchange_order_type,omitempty"`
	Quantity           int64         `json:"quantity"`
	InstrumentUID      string        `json:"instrument_uid,omitempty"`
	Ticker             string        `json:"ticker,omitempty"`
	ClassCode          string        `json:"class_code,omitempty"`
	Currency           string        `json:"currency,omitempty"`
	Price              *Decimal      `json:"price,omitempty"`
	StopPrice          *Decimal      `json:"stop_price,omitempty"`
	TakeProfitType     string        `json:"take_profit_type,omitempty"`
	Trailing           *TrailingView `json:"trailing,omitempty"`
	ExchangeOrderID    string        `json:"exchange_order_id,omitempty"`
	CreateDate         string        `json:"create_date,omitempty"`
	ActivationDateTime string        `json:"activation_date_time,omitempty"`
	ExpirationTime     string        `json:"expiration_time,omitempty"`
}

// StopOrder converts a StopOrder. status is passed in so callers use the
// shared status mapping (internal/broker/stoporders.StatusName) rather than
// duplicating the enum switch here.
func StopOrder(s *investapi.StopOrder, status string) StopOrderView {
	return StopOrderView{
		StopOrderID:        s.GetStopOrderId(),
		Status:             status,
		Direction:          s.GetDirection().String(),
		StopOrderType:      s.GetOrderType().String(),
		ExchangeOrderType:  s.GetExchangeOrderType().String(),
		Quantity:           s.GetLotsRequested(),
		InstrumentUID:      s.GetInstrumentUid(),
		Ticker:             s.GetTicker(),
		ClassCode:          s.GetClassCode(),
		Currency:           s.GetCurrency(),
		Price:              moneyPtr(s.GetPrice()),
		StopPrice:          moneyPtr(s.GetStopPrice()),
		TakeProfitType:     s.GetTakeProfitType().String(),
		Trailing:           trailingView(s.GetTrailingData()),
		ExchangeOrderID:    s.GetExchangeOrderId(),
		CreateDate:         Timestamp(s.GetCreateDate()),
		ActivationDateTime: Timestamp(s.GetActivationDateTime()),
		ExpirationTime:     Timestamp(s.GetExpirationTime()),
	}
}

// StopOrdersTable renders a stop-order list for humans.
func StopOrdersTable(w io.Writer, views []StopOrderView) error {
	rows := make([][]string, 0, len(views))
	for _, v := range views {
		sp := ""
		if v.StopPrice != nil {
			sp = v.StopPrice.Value
		}
		rows = append(rows, []string{v.StopOrderID, v.Status, v.Direction, v.StopOrderType, v.Ticker, itoa(v.Quantity), sp})
	}
	return Table(w, []string{"STOP_ORDER_ID", "STATUS", "DIRECTION", "TYPE", "TICKER", "QUANTITY", "STOP_PRICE"}, rows)
}

func quotationPtr(q *investapi.Quotation) *Decimal {
	if q == nil {
		return nil
	}
	d := Quotation(q)
	return &d
}
