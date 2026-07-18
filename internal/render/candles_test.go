package render

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	investapi "tinvest/internal/pb/investapi"
)

func TestCandlesPreserveCompletenessAndVolumes(t *testing.T) {
	views := Candles([]*investapi.HistoricCandle{{
		Open: &investapi.Quotation{Units: 100}, High: &investapi.Quotation{Units: 110},
		Low: &investapi.Quotation{Units: 90}, Close: &investapi.Quotation{Units: 105},
		Volume: 123, VolumeBuy: 80, VolumeSell: 43, IsComplete: false,
		Time:         timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		CandleSource: investapi.CandleSource_CANDLE_SOURCE_EXCHANGE,
	}})
	if len(views) != 1 || views[0].Close.Value != "105" || views[0].Volume != "123" {
		t.Fatalf("candles = %+v", views)
	}
	if views[0].IsComplete || views[0].Source != "CANDLE_SOURCE_EXCHANGE" {
		t.Errorf("candle = %+v", views[0])
	}
}
