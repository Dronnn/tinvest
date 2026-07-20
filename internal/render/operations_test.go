package render

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

func TestOperationsAndExecutedTradesViews(t *testing.T) {
	date := timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	items := []*investapi.OperationItem{{
		Cursor: "row-1", Id: "op-1", Name: "Buy", Date: date,
		Type: investapi.OperationType_OPERATION_TYPE_BUY, State: investapi.OperationState_OPERATION_STATE_EXECUTED,
		InstrumentUid: "uid-1", Ticker: "SBER", Quantity: 10, QuantityDone: 10,
		Payment: &investapi.MoneyValue{Currency: "rub", Units: -2500},
		TradesInfo: &investapi.OperationItemTrades{Trades: []*investapi.OperationItemTrade{{
			Num: "trade-1", Date: date, Quantity: 10,
			Price: &investapi.MoneyValue{Currency: "rub", Units: 250},
		}}},
	}}

	operations := Operations(items)
	if len(operations) != 1 || operations[0].ID != "op-1" || operations[0].Quantity != "10" {
		t.Fatalf("operations = %+v", operations)
	}
	if operations[0].Payment.Value != "-2500" || operations[0].State != "OPERATION_STATE_EXECUTED" {
		t.Errorf("operation = %+v", operations[0])
	}

	trades := ExecutedTrades(items)
	if len(trades) != 1 || trades[0].TradeID != "trade-1" || trades[0].OperationID != "op-1" {
		t.Fatalf("trades = %+v", trades)
	}
	if trades[0].Price.Value != "250" || trades[0].Quantity != "10" {
		t.Errorf("trade = %+v", trades[0])
	}
}

func TestPaginationTableEmitsNextCursor(t *testing.T) {
	var output bytes.Buffer
	if err := PaginationTable(&output, "page-2"); err != nil {
		t.Fatalf("PaginationTable: %v", err)
	}
	if !strings.Contains(output.String(), "NEXT_CURSOR") || !strings.Contains(output.String(), "page-2") {
		t.Fatalf("table = %q", output.String())
	}
}
