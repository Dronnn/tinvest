package render

import (
	"testing"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

func TestTariffAndMarginViews(t *testing.T) {
	perSecond := int32(5)
	tariff := Tariff(&investapi.GetUserTariffResponse{
		UnaryLimits:  []*investapi.UnaryLimit{{LimitPerMinute: 100, LimitPerSecond: &perSecond, Methods: []string{"GetCandles"}}},
		StreamLimits: []*investapi.StreamLimit{{Limit: 10, Open: 2, Streams: []string{"MarketDataStream"}}},
	})
	if len(tariff.UnaryLimits) != 1 || *tariff.UnaryLimits[0].LimitPerSecond != 5 {
		t.Fatalf("tariff = %+v", tariff)
	}
	if len(tariff.StreamLimits) != 1 || tariff.StreamLimits[0].Open != 2 {
		t.Errorf("tariff = %+v", tariff)
	}

	margin := Margin(&investapi.GetMarginAttributesResponse{
		LiquidPortfolio:       &investapi.MoneyValue{Currency: "rub", Units: 1000},
		FundsSufficiencyLevel: &investapi.Quotation{Units: 2},
	})
	if margin.LiquidPortfolio.Value != "1000" || margin.FundsSufficiencyLevel.Value != "2" {
		t.Errorf("margin = %+v", margin)
	}
}

func TestSignalViews(t *testing.T) {
	strategies := SignalStrategies([]*investapi.Strategy{{
		StrategyId: "strategy-1", StrategyName: "Momentum", TimeInPosition: 3600,
		Yield: &investapi.Quotation{Units: 12},
	}})
	if len(strategies) != 1 || strategies[0].TimeInPositionSeconds != "3600" || strategies[0].Yield.Value != "12" {
		t.Fatalf("strategies = %+v", strategies)
	}

	probability := int32(80)
	signals := Signals([]*investapi.Signal{{
		SignalId: "signal-1", StrategyId: "strategy-1", InstrumentUid: "uid-1",
		Direction: investapi.SignalDirection_SIGNAL_DIRECTION_BUY, Probability: &probability,
		InitialPrice: &investapi.Quotation{Units: 100}, TargetPrice: &investapi.Quotation{Units: 110},
	}})
	if len(signals) != 1 || signals[0].Direction != "SIGNAL_DIRECTION_BUY" || *signals[0].Probability != 80 {
		t.Fatalf("signals = %+v", signals)
	}
}
