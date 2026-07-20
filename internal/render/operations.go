package render

import (
	"fmt"
	"io"
	"strconv"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// OperationView is one GetOperationsByCursor item.
type OperationView struct {
	Cursor            string  `json:"cursor"`
	ID                string  `json:"id"`
	ParentOperationID string  `json:"parent_operation_id,omitempty"`
	Name              string  `json:"name"`
	Description       string  `json:"description,omitempty"`
	Date              string  `json:"date"`
	Type              string  `json:"type"`
	State             string  `json:"state"`
	InstrumentUID     string  `json:"instrument_uid,omitempty"`
	PositionUID       string  `json:"position_uid,omitempty"`
	FIGI              string  `json:"figi,omitempty"`
	Ticker            string  `json:"ticker,omitempty"`
	ClassCode         string  `json:"class_code,omitempty"`
	InstrumentType    string  `json:"instrument_type,omitempty"`
	InstrumentKind    string  `json:"instrument_kind"`
	Payment           Decimal `json:"payment"`
	Price             Decimal `json:"price"`
	Commission        Decimal `json:"commission"`
	Yield             Decimal `json:"yield"`
	YieldRelative     Decimal `json:"yield_relative"`
	AccruedInterest   Decimal `json:"accrued_interest"`
	Quantity          string  `json:"quantity"`
	QuantityRest      string  `json:"quantity_rest"`
	QuantityDone      string  `json:"quantity_done"`
	CancelDate        string  `json:"cancel_date,omitempty"`
	CancelReason      string  `json:"cancel_reason,omitempty"`
	TradeCount        int     `json:"trade_count"`
}

// Operations converts operation items, preserving broker order.
func Operations(items []*investapi.OperationItem) []OperationView {
	views := make([]OperationView, 0, len(items))
	for _, item := range items {
		views = append(views, OperationView{
			Cursor: item.GetCursor(), ID: item.GetId(), ParentOperationID: item.GetParentOperationId(),
			Name: item.GetName(), Description: item.GetDescription(), Date: Timestamp(item.GetDate()),
			Type: item.GetType().String(), State: item.GetState().String(),
			InstrumentUID: item.GetInstrumentUid(), PositionUID: item.GetPositionUid(), FIGI: item.GetFigi(),
			Ticker: item.GetTicker(), ClassCode: item.GetClassCode(), InstrumentType: item.GetInstrumentType(),
			InstrumentKind: item.GetInstrumentKind().String(), Payment: Money(item.GetPayment()), Price: Money(item.GetPrice()),
			Commission: Money(item.GetCommission()), Yield: Money(item.GetYield()), YieldRelative: Quotation(item.GetYieldRelative()),
			AccruedInterest: Money(item.GetAccruedInt()), Quantity: strconv.FormatInt(item.GetQuantity(), 10),
			QuantityRest: strconv.FormatInt(item.GetQuantityRest(), 10), QuantityDone: strconv.FormatInt(item.GetQuantityDone(), 10),
			CancelDate: Timestamp(item.GetCancelDateTime()), CancelReason: item.GetCancelReason(),
			TradeCount: len(item.GetTradesInfo().GetTrades()),
		})
	}
	return views
}

// OperationsTable renders cursor-paginated operation history.
func OperationsTable(w io.Writer, views []OperationView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.Date, view.ID, view.Type, view.State, view.Ticker, view.Payment.Value, view.QuantityDone})
	}
	return Table(w, []string{"DATE", "ID", "TYPE", "STATE", "TICKER", "PAYMENT", "DONE"}, rows)
}

// PaginationTable emits the continuation cursor after a table response.
func PaginationTable(w io.Writer, nextCursor string) error {
	if nextCursor == "" {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return Table(w, []string{"NEXT_CURSOR"}, [][]string{{nextCursor}})
}

// ExecutedTradeView is one execution nested under an operation item.
type ExecutedTradeView struct {
	OperationID   string  `json:"operation_id"`
	OperationType string  `json:"operation_type"`
	InstrumentUID string  `json:"instrument_uid,omitempty"`
	FIGI          string  `json:"figi,omitempty"`
	Ticker        string  `json:"ticker,omitempty"`
	ClassCode     string  `json:"class_code,omitempty"`
	TradeID       string  `json:"trade_id"`
	Date          string  `json:"date"`
	Quantity      string  `json:"quantity"`
	Price         Decimal `json:"price"`
	Yield         Decimal `json:"yield"`
	YieldRelative Decimal `json:"yield_relative"`
}

// ExecutedTrades flattens the trades nested under executed operation items.
func ExecutedTrades(items []*investapi.OperationItem) []ExecutedTradeView {
	views := make([]ExecutedTradeView, 0)
	for _, item := range items {
		for _, trade := range item.GetTradesInfo().GetTrades() {
			views = append(views, ExecutedTradeView{
				OperationID: item.GetId(), OperationType: item.GetType().String(), InstrumentUID: item.GetInstrumentUid(),
				FIGI: item.GetFigi(), Ticker: item.GetTicker(), ClassCode: item.GetClassCode(), TradeID: trade.GetNum(),
				Date: Timestamp(trade.GetDate()), Quantity: strconv.FormatInt(trade.GetQuantity(), 10),
				Price: Money(trade.GetPrice()), Yield: Money(trade.GetYield()), YieldRelative: Quotation(trade.GetYieldRelative()),
			})
		}
	}
	return views
}

// ExecutedTradesTable renders flattened executions.
func ExecutedTradesTable(w io.Writer, views []ExecutedTradeView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.Date, view.TradeID, view.OperationID, view.Ticker, view.Quantity, view.Price.Value})
	}
	return Table(w, []string{"DATE", "TRADE_ID", "OPERATION_ID", "TICKER", "QUANTITY", "PRICE"}, rows)
}
