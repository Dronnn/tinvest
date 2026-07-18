package render

import (
	"bytes"
	"strings"
	"testing"

	investapi "tinvest/internal/pb/investapi"
)

func TestBalanceSummarizesMoneyByCurrency(t *testing.T) {
	view := Balance(&investapi.WithdrawLimitsResponse{
		Money: []*investapi.MoneyValue{
			{Currency: "usd", Units: 10},
			{Currency: "rub", Units: 100, Nano: 500_000_000},
			{Currency: "rub", Units: 20, Nano: 500_000_000},
		},
		Blocked: []*investapi.MoneyValue{
			{Currency: "rub", Units: 5},
		},
		BlockedGuarantee: []*investapi.MoneyValue{
			{Currency: "rub", Units: 2},
		},
	})

	if len(view.Currencies) != 2 {
		t.Fatalf("currencies = %+v", view.Currencies)
	}
	if view.Currencies[0].Currency != "rub" || view.Currencies[0].Available.Value != "121" {
		t.Errorf("rub summary = %+v", view.Currencies[0])
	}
	if view.Currencies[0].Blocked.Value != "5" || view.Currencies[0].BlockedGuarantee.Value != "2" {
		t.Errorf("rub blocked summary = %+v", view.Currencies[0])
	}
	if view.Currencies[1].Currency != "usd" || view.Currencies[1].Available.Value != "10" {
		t.Errorf("usd summary = %+v", view.Currencies[1])
	}
}

func TestPortfolioViewPreservesPositionValues(t *testing.T) {
	view := Portfolio(&investapi.PortfolioResponse{
		AccountId:            "acc-1",
		TotalAmountPortfolio: &investapi.MoneyValue{Currency: "rub", Units: 1_000},
		ExpectedYield:        &investapi.Quotation{Units: 12, Nano: 500_000_000},
		Positions: []*investapi.PortfolioPosition{{
			InstrumentUid:    "uid-1",
			Ticker:           "SBER",
			Quantity:         &investapi.Quotation{Units: 10},
			CurrentPrice:     &investapi.MoneyValue{Currency: "rub", Units: 250},
			Blocked:          true,
			VarMarginSettled: &investapi.MoneyValue{Currency: "rub", Units: 7},
		}},
		VirtualPositions: []*investapi.VirtualPortfolioPosition{{
			InstrumentUid: "virtual-uid", Ticker: "VIRTUAL", Quantity: &investapi.Quotation{Units: 2},
		}},
	})

	if view.AccountID != "acc-1" || view.TotalAmountPortfolio.Value != "1000" {
		t.Fatalf("portfolio = %+v", view)
	}
	if view.ExpectedYield.Value != "12.5" || len(view.Positions) != 1 {
		t.Fatalf("portfolio = %+v", view)
	}
	if view.Positions[0].InstrumentUID != "uid-1" || !view.Positions[0].Blocked {
		t.Errorf("position = %+v", view.Positions[0])
	}
	if view.Positions[0].VariationMarginSettled.Value != "7" {
		t.Errorf("settled variation margin = %+v", view.Positions[0].VariationMarginSettled)
	}
	if len(view.VirtualPositions) != 1 || view.VirtualPositions[0].InstrumentUID != "virtual-uid" {
		t.Errorf("virtual positions = %+v", view.VirtualPositions)
	}
}

func TestPositionsTableIncludesBlockedMoney(t *testing.T) {
	view := Positions(&investapi.PositionsResponse{
		Money:   []*investapi.MoneyValue{{Currency: "rub", Units: 100}},
		Blocked: []*investapi.MoneyValue{{Currency: "rub", Units: 12}},
	})
	var output bytes.Buffer
	if err := PositionsTable(&output, view); err != nil {
		t.Fatalf("PositionsTable: %v", err)
	}
	if !strings.Contains(output.String(), "blocked_money") || !strings.Contains(output.String(), "12") {
		t.Fatalf("table = %q", output.String())
	}
}
