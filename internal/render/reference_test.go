package render

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	brokerinstruments "github.com/Dronnn/tinvest/internal/broker/instruments"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

func TestReferenceDataViews(t *testing.T) {
	listed := ListedInstruments([]brokerinstruments.ListedInstrument{{
		UID: "uid-1", Ticker: "SBER", Type: "share", Lot: 10,
		TradingStatus: investapi.SecurityTradingStatus_SECURITY_TRADING_STATUS_NORMAL_TRADING,
	}})
	if len(listed) != 1 || listed[0].Type != "share" || listed[0].Lot != 10 {
		t.Fatalf("listed = %+v", listed)
	}

	date := timestamppb.New(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	dividends := Dividends([]*investapi.Dividend{{DividendNet: &investapi.MoneyValue{Currency: "rub", Units: 25}, RecordDate: date}})
	if dividends[0].DividendNet.Value != "25" || dividends[0].RecordDate != "2026-01-02T00:00:00Z" {
		t.Errorf("dividend = %+v", dividends[0])
	}
	coupons := Coupons([]*investapi.Coupon{{CouponNumber: 7, CouponDate: date, PayOneBond: &investapi.MoneyValue{Currency: "rub", Units: 12}}})
	if coupons[0].CouponNumber != "7" || coupons[0].PayOneBond.Value != "12" {
		t.Errorf("coupon = %+v", coupons[0])
	}
	accrued := AccruedInterests([]*investapi.AccruedInterest{{Date: date, Value: &investapi.Quotation{Units: 3}}})
	if accrued[0].Value.Value != "3" {
		t.Errorf("accrued = %+v", accrued[0])
	}

	schedules := TradingSchedules([]*investapi.TradingSchedule{{Exchange: "MOEX", Days: []*investapi.TradingDay{{Date: date, IsTradingDay: true}}}})
	if len(schedules) != 1 || schedules[0].Exchange != "MOEX" || !schedules[0].IsTradingDay {
		t.Errorf("schedules = %+v", schedules)
	}
	status := TradingStatus(&investapi.GetTradingStatusResponse{
		InstrumentUid: "uid-1", TradingStatus: investapi.SecurityTradingStatus_SECURITY_TRADING_STATUS_NORMAL_TRADING,
		LimitOrderAvailableFlag: true,
	})
	if status.InstrumentUID != "uid-1" || !status.LimitOrderAvailable {
		t.Errorf("status = %+v", status)
	}
}
