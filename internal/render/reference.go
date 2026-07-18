package render

import (
	"io"
	"strconv"

	brokerinstruments "tinvest/internal/broker/instruments"
	investapi "tinvest/internal/pb/investapi"
)

// ListedInstrumentView is the common shape of all per-type list RPCs.
type ListedInstrumentView struct {
	UID           string `json:"uid"`
	FIGI          string `json:"figi,omitempty"`
	Ticker        string `json:"ticker"`
	ClassCode     string `json:"class_code"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Lot           int32  `json:"lot"`
	Currency      string `json:"currency"`
	TradingStatus string `json:"trading_status"`
}

// ListedInstruments converts common broker list records.
func ListedInstruments(instruments []brokerinstruments.ListedInstrument) []ListedInstrumentView {
	views := make([]ListedInstrumentView, 0, len(instruments))
	for _, instrument := range instruments {
		views = append(views, ListedInstrumentView{
			UID: instrument.UID, FIGI: instrument.FIGI, Ticker: instrument.Ticker, ClassCode: instrument.ClassCode,
			Name: instrument.Name, Type: instrument.Type, Lot: instrument.Lot, Currency: instrument.Currency,
			TradingStatus: instrument.TradingStatus.String(),
		})
	}
	return views
}

// ListedInstrumentsTable renders per-type instrument lists.
func ListedInstrumentsTable(w io.Writer, views []ListedInstrumentView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.UID, view.Ticker, view.ClassCode, view.Type, view.Name, strconv.Itoa(int(view.Lot)), view.Currency})
	}
	return Table(w, []string{"UID", "TICKER", "CLASS_CODE", "TYPE", "NAME", "LOT", "CURRENCY"}, rows)
}

// DividendView is one dividend event.
type DividendView struct {
	DividendNet  Decimal `json:"dividend_net"`
	PaymentDate  string  `json:"payment_date,omitempty"`
	DeclaredDate string  `json:"declared_date,omitempty"`
	LastBuyDate  string  `json:"last_buy_date,omitempty"`
	RecordDate   string  `json:"record_date,omitempty"`
	Type         string  `json:"type,omitempty"`
	Regularity   string  `json:"regularity,omitempty"`
	ClosePrice   Decimal `json:"close_price"`
	Yield        Decimal `json:"yield"`
	CreatedAt    string  `json:"created_at,omitempty"`
}

// Dividends converts dividend events.
func Dividends(dividends []*investapi.Dividend) []DividendView {
	views := make([]DividendView, 0, len(dividends))
	for _, dividend := range dividends {
		views = append(views, DividendView{
			DividendNet: Money(dividend.GetDividendNet()), PaymentDate: Timestamp(dividend.GetPaymentDate()),
			DeclaredDate: Timestamp(dividend.GetDeclaredDate()), LastBuyDate: Timestamp(dividend.GetLastBuyDate()),
			RecordDate: Timestamp(dividend.GetRecordDate()), Type: dividend.GetDividendType(), Regularity: dividend.GetRegularity(),
			ClosePrice: Money(dividend.GetClosePrice()), Yield: Quotation(dividend.GetYieldValue()), CreatedAt: Timestamp(dividend.GetCreatedAt()),
		})
	}
	return views
}

// DividendsTable renders dividend events.
func DividendsTable(w io.Writer, views []DividendView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.RecordDate, view.PaymentDate, view.DividendNet.Value, view.DividendNet.Currency, view.Yield.Value, view.Type})
	}
	return Table(w, []string{"RECORD_DATE", "PAYMENT_DATE", "NET", "CURRENCY", "YIELD", "TYPE"}, rows)
}

// CouponView is one bond coupon event.
type CouponView struct {
	FIGI            string  `json:"figi,omitempty"`
	CouponDate      string  `json:"coupon_date,omitempty"`
	CouponNumber    string  `json:"coupon_number"`
	FixDate         string  `json:"fix_date,omitempty"`
	PayOneBond      Decimal `json:"pay_one_bond"`
	CouponType      string  `json:"coupon_type"`
	CouponStartDate string  `json:"coupon_start_date,omitempty"`
	CouponEndDate   string  `json:"coupon_end_date,omitempty"`
	CouponPeriod    int32   `json:"coupon_period_days"`
}

// Coupons converts bond coupon events.
func Coupons(coupons []*investapi.Coupon) []CouponView {
	views := make([]CouponView, 0, len(coupons))
	for _, coupon := range coupons {
		views = append(views, CouponView{
			FIGI: coupon.GetFigi(), CouponDate: Timestamp(coupon.GetCouponDate()), CouponNumber: strconv.FormatInt(coupon.GetCouponNumber(), 10),
			FixDate: Timestamp(coupon.GetFixDate()), PayOneBond: Money(coupon.GetPayOneBond()), CouponType: coupon.GetCouponType().String(),
			CouponStartDate: Timestamp(coupon.GetCouponStartDate()), CouponEndDate: Timestamp(coupon.GetCouponEndDate()), CouponPeriod: coupon.GetCouponPeriod(),
		})
	}
	return views
}

// CouponsTable renders coupon events.
func CouponsTable(w io.Writer, views []CouponView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.CouponDate, view.CouponNumber, view.PayOneBond.Value, view.PayOneBond.Currency, view.CouponType})
	}
	return Table(w, []string{"DATE", "NUMBER", "PAYMENT", "CURRENCY", "TYPE"}, rows)
}

// AccruedInterestView is one accrued-interest value.
type AccruedInterestView struct {
	Date         string  `json:"date"`
	Value        Decimal `json:"value"`
	ValuePercent Decimal `json:"value_percent"`
	Nominal      Decimal `json:"nominal"`
}

// AccruedInterests converts accrued-interest values.
func AccruedInterests(values []*investapi.AccruedInterest) []AccruedInterestView {
	views := make([]AccruedInterestView, 0, len(values))
	for _, value := range values {
		views = append(views, AccruedInterestView{
			Date: Timestamp(value.GetDate()), Value: Quotation(value.GetValue()),
			ValuePercent: Quotation(value.GetValuePercent()), Nominal: Quotation(value.GetNominal()),
		})
	}
	return views
}

// AccruedInterestsTable renders accrued-interest values.
func AccruedInterestsTable(w io.Writer, views []AccruedInterestView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.Date, view.Value.Value, view.ValuePercent.Value, view.Nominal.Value})
	}
	return Table(w, []string{"DATE", "VALUE", "PERCENT", "NOMINAL"}, rows)
}

// TradingScheduleView is one exchange/day calendar row.
type TradingScheduleView struct {
	Exchange       string `json:"exchange"`
	Date           string `json:"date"`
	IsTradingDay   bool   `json:"is_trading_day"`
	StartTime      string `json:"start_time,omitempty"`
	EndTime        string `json:"end_time,omitempty"`
	PremarketStart string `json:"premarket_start_time,omitempty"`
	PremarketEnd   string `json:"premarket_end_time,omitempty"`
	EveningStart   string `json:"evening_start_time,omitempty"`
	EveningEnd     string `json:"evening_end_time,omitempty"`
}

// TradingSchedules flattens exchange schedules into day rows.
func TradingSchedules(schedules []*investapi.TradingSchedule) []TradingScheduleView {
	views := make([]TradingScheduleView, 0)
	for _, schedule := range schedules {
		for _, day := range schedule.GetDays() {
			views = append(views, TradingScheduleView{
				Exchange: schedule.GetExchange(), Date: Timestamp(day.GetDate()), IsTradingDay: day.GetIsTradingDay(),
				StartTime: Timestamp(day.GetStartTime()), EndTime: Timestamp(day.GetEndTime()),
				PremarketStart: Timestamp(day.GetPremarketStartTime()), PremarketEnd: Timestamp(day.GetPremarketEndTime()),
				EveningStart: Timestamp(day.GetEveningStartTime()), EveningEnd: Timestamp(day.GetEveningEndTime()),
			})
		}
	}
	return views
}

// TradingSchedulesTable renders exchange/day rows.
func TradingSchedulesTable(w io.Writer, views []TradingScheduleView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.Exchange, view.Date, strconv.FormatBool(view.IsTradingDay), view.StartTime, view.EndTime})
	}
	return Table(w, []string{"EXCHANGE", "DATE", "TRADING", "START", "END"}, rows)
}

// TradingStatusView is the current order/trading availability for one instrument.
type TradingStatusView struct {
	InstrumentUID        string `json:"instrument_uid"`
	FIGI                 string `json:"figi,omitempty"`
	Ticker               string `json:"ticker,omitempty"`
	ClassCode            string `json:"class_code,omitempty"`
	TradingStatus        string `json:"trading_status"`
	LimitOrderAvailable  bool   `json:"limit_order_available"`
	MarketOrderAvailable bool   `json:"market_order_available"`
	BestPriceAvailable   bool   `json:"bestprice_order_available"`
	APITradeAvailable    bool   `json:"api_trade_available"`
	OnlyBestPrice        bool   `json:"only_best_price"`
}

// TradingStatus converts a current trading-status response.
func TradingStatus(status *investapi.GetTradingStatusResponse) TradingStatusView {
	return TradingStatusView{
		InstrumentUID: status.GetInstrumentUid(), FIGI: status.GetFigi(), Ticker: status.GetTicker(), ClassCode: status.GetClassCode(),
		TradingStatus: status.GetTradingStatus().String(), LimitOrderAvailable: status.GetLimitOrderAvailableFlag(),
		MarketOrderAvailable: status.GetMarketOrderAvailableFlag(), BestPriceAvailable: status.GetBestpriceOrderAvailableFlag(),
		APITradeAvailable: status.GetApiTradeAvailableFlag(), OnlyBestPrice: status.GetOnlyBestPrice(),
	}
}

// TradingStatusTable renders current trading availability as key/value rows.
func TradingStatusTable(w io.Writer, view TradingStatusView) error {
	return Table(w, []string{"FIELD", "VALUE"}, [][]string{
		{"instrument_uid", view.InstrumentUID}, {"ticker", view.Ticker}, {"class_code", view.ClassCode},
		{"trading_status", view.TradingStatus}, {"limit_order_available", strconv.FormatBool(view.LimitOrderAvailable)},
		{"market_order_available", strconv.FormatBool(view.MarketOrderAvailable)},
		{"bestprice_order_available", strconv.FormatBool(view.BestPriceAvailable)},
		{"api_trade_available", strconv.FormatBool(view.APITradeAvailable)},
	})
}
