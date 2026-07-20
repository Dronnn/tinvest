package render

import (
	"fmt"
	"io"
	"sort"
	"strconv"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// PortfolioPositionView is one instrument position in a portfolio snapshot.
type PortfolioPositionView struct {
	InstrumentUID            string  `json:"instrument_uid"`
	PositionUID              string  `json:"position_uid"`
	FIGI                     string  `json:"figi"`
	Ticker                   string  `json:"ticker"`
	ClassCode                string  `json:"class_code"`
	InstrumentType           string  `json:"instrument_type"`
	Quantity                 Decimal `json:"quantity"`
	AveragePositionPrice     Decimal `json:"average_position_price"`
	AveragePositionPriceFIFO Decimal `json:"average_position_price_fifo"`
	CurrentPrice             Decimal `json:"current_price"`
	ExpectedYield            Decimal `json:"expected_yield"`
	ExpectedYieldFIFO        Decimal `json:"expected_yield_fifo"`
	DailyYield               Decimal `json:"daily_yield"`
	CurrentAccruedInterest   Decimal `json:"current_accrued_interest"`
	Blocked                  bool    `json:"blocked"`
	BlockedLots              Decimal `json:"blocked_lots"`
	VariationMargin          Decimal `json:"variation_margin"`
	VariationMarginSettled   Decimal `json:"variation_margin_settled"`
}

// VirtualPortfolioPositionView is one expiring virtual instrument position.
type VirtualPortfolioPositionView struct {
	InstrumentUID            string  `json:"instrument_uid"`
	PositionUID              string  `json:"position_uid"`
	FIGI                     string  `json:"figi"`
	Ticker                   string  `json:"ticker"`
	ClassCode                string  `json:"class_code"`
	InstrumentType           string  `json:"instrument_type"`
	Quantity                 Decimal `json:"quantity"`
	AveragePositionPrice     Decimal `json:"average_position_price"`
	AveragePositionPriceFIFO Decimal `json:"average_position_price_fifo"`
	CurrentPrice             Decimal `json:"current_price"`
	ExpectedYield            Decimal `json:"expected_yield"`
	ExpectedYieldFIFO        Decimal `json:"expected_yield_fifo"`
	DailyYield               Decimal `json:"daily_yield"`
	ExpireDate               string  `json:"expire_date,omitempty"`
}

// PortfolioView is the stable JSON shape of `portfolio get`.
type PortfolioView struct {
	AccountID             string                         `json:"account_id"`
	TotalAmountPortfolio  Decimal                        `json:"total_amount_portfolio"`
	TotalAmountShares     Decimal                        `json:"total_amount_shares"`
	TotalAmountBonds      Decimal                        `json:"total_amount_bonds"`
	TotalAmountETF        Decimal                        `json:"total_amount_etf"`
	TotalAmountCurrencies Decimal                        `json:"total_amount_currencies"`
	TotalAmountFutures    Decimal                        `json:"total_amount_futures"`
	TotalAmountOptions    Decimal                        `json:"total_amount_options"`
	TotalAmountSP         Decimal                        `json:"total_amount_sp"`
	TotalAmountDFA        Decimal                        `json:"total_amount_dfa"`
	ExpectedYield         Decimal                        `json:"expected_yield"`
	DailyYield            Decimal                        `json:"daily_yield"`
	DailyYieldRelative    Decimal                        `json:"daily_yield_relative"`
	Positions             []PortfolioPositionView        `json:"positions"`
	VirtualPositions      []VirtualPortfolioPositionView `json:"virtual_positions"`
}

// Portfolio converts an API portfolio snapshot without using floating point.
func Portfolio(p *investapi.PortfolioResponse) PortfolioView {
	positions := make([]PortfolioPositionView, 0, len(p.GetPositions()))
	for _, position := range p.GetPositions() {
		positions = append(positions, PortfolioPositionView{
			InstrumentUID:            position.GetInstrumentUid(),
			PositionUID:              position.GetPositionUid(),
			FIGI:                     position.GetFigi(),
			Ticker:                   position.GetTicker(),
			ClassCode:                position.GetClassCode(),
			InstrumentType:           position.GetInstrumentType(),
			Quantity:                 Quotation(position.GetQuantity()),
			AveragePositionPrice:     Money(position.GetAveragePositionPrice()),
			AveragePositionPriceFIFO: Money(position.GetAveragePositionPriceFifo()),
			CurrentPrice:             Money(position.GetCurrentPrice()),
			ExpectedYield:            Quotation(position.GetExpectedYield()),
			ExpectedYieldFIFO:        Quotation(position.GetExpectedYieldFifo()),
			DailyYield:               Money(position.GetDailyYield()),
			CurrentAccruedInterest:   Money(position.GetCurrentNkd()),
			Blocked:                  position.GetBlocked(),
			BlockedLots:              Quotation(position.GetBlockedLots()),
			VariationMargin:          Money(position.GetVarMargin()),
			VariationMarginSettled:   Money(position.GetVarMarginSettled()),
		})
	}
	virtualPositions := make([]VirtualPortfolioPositionView, 0, len(p.GetVirtualPositions()))
	for _, position := range p.GetVirtualPositions() {
		virtualPositions = append(virtualPositions, VirtualPortfolioPositionView{
			InstrumentUID: position.GetInstrumentUid(), PositionUID: position.GetPositionUid(), FIGI: position.GetFigi(),
			Ticker: position.GetTicker(), ClassCode: position.GetClassCode(), InstrumentType: position.GetInstrumentType(),
			Quantity: Quotation(position.GetQuantity()), AveragePositionPrice: Money(position.GetAveragePositionPrice()),
			AveragePositionPriceFIFO: Money(position.GetAveragePositionPriceFifo()), CurrentPrice: Money(position.GetCurrentPrice()),
			ExpectedYield: Quotation(position.GetExpectedYield()), ExpectedYieldFIFO: Quotation(position.GetExpectedYieldFifo()),
			DailyYield: Money(position.GetDailyYield()), ExpireDate: Timestamp(position.GetExpireDate()),
		})
	}
	return PortfolioView{
		AccountID:             p.GetAccountId(),
		TotalAmountPortfolio:  Money(p.GetTotalAmountPortfolio()),
		TotalAmountShares:     Money(p.GetTotalAmountShares()),
		TotalAmountBonds:      Money(p.GetTotalAmountBonds()),
		TotalAmountETF:        Money(p.GetTotalAmountEtf()),
		TotalAmountCurrencies: Money(p.GetTotalAmountCurrencies()),
		TotalAmountFutures:    Money(p.GetTotalAmountFutures()),
		TotalAmountOptions:    Money(p.GetTotalAmountOptions()),
		TotalAmountSP:         Money(p.GetTotalAmountSp()),
		TotalAmountDFA:        Money(p.GetTotalAmountDfa()),
		ExpectedYield:         Quotation(p.GetExpectedYield()),
		DailyYield:            Money(p.GetDailyYield()),
		DailyYieldRelative:    Quotation(p.GetDailyYieldRelative()),
		Positions:             positions,
		VirtualPositions:      virtualPositions,
	}
}

// PortfolioTable renders totals followed by the instrument positions.
func PortfolioTable(w io.Writer, view PortfolioView) error {
	if err := Table(w, []string{"FIELD", "VALUE"}, [][]string{
		{"account_id", view.AccountID},
		{"total_portfolio", view.TotalAmountPortfolio.Value + " " + view.TotalAmountPortfolio.Currency},
		{"expected_yield", view.ExpectedYield.Value},
		{"daily_yield", view.DailyYield.Value + " " + view.DailyYield.Currency},
	}); err != nil {
		return err
	}
	if len(view.Positions) == 0 && len(view.VirtualPositions) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	rows := make([][]string, 0, len(view.Positions)+len(view.VirtualPositions))
	for _, position := range view.Positions {
		rows = append(rows, []string{
			"position", position.InstrumentUID, position.Ticker, position.InstrumentType,
			position.Quantity.Value, position.CurrentPrice.Value,
			position.ExpectedYield.Value, strconv.FormatBool(position.Blocked),
		})
	}
	for _, position := range view.VirtualPositions {
		rows = append(rows, []string{
			"virtual", position.InstrumentUID, position.Ticker, position.InstrumentType,
			position.Quantity.Value, position.CurrentPrice.Value, position.ExpectedYield.Value, position.ExpireDate,
		})
	}
	return Table(w, []string{"KIND", "UID", "TICKER", "TYPE", "QUANTITY", "PRICE", "YIELD", "BLOCKED/EXPIRES"}, rows)
}

// SecurityPositionView is one security balance returned by GetPositions.
type SecurityPositionView struct {
	InstrumentUID   string `json:"instrument_uid"`
	PositionUID     string `json:"position_uid"`
	FIGI            string `json:"figi"`
	Ticker          string `json:"ticker"`
	ClassCode       string `json:"class_code"`
	InstrumentType  string `json:"instrument_type"`
	Balance         string `json:"balance"`
	Blocked         string `json:"blocked"`
	ExchangeBlocked bool   `json:"exchange_blocked"`
}

// DerivativePositionView is a futures or options position.
type DerivativePositionView struct {
	InstrumentUID string `json:"instrument_uid"`
	PositionUID   string `json:"position_uid"`
	FIGI          string `json:"figi,omitempty"`
	Ticker        string `json:"ticker"`
	ClassCode     string `json:"class_code"`
	Balance       string `json:"balance"`
	Blocked       string `json:"blocked"`
}

// PositionsView is the stable JSON shape of `positions get`.
type PositionsView struct {
	AccountID               string                   `json:"account_id"`
	Money                   []Decimal                `json:"money"`
	BlockedMoney            []Decimal                `json:"blocked_money"`
	Securities              []SecurityPositionView   `json:"securities"`
	Futures                 []DerivativePositionView `json:"futures"`
	Options                 []DerivativePositionView `json:"options"`
	LimitsLoadingInProgress bool                     `json:"limits_loading_in_progress"`
}

// Positions converts an API positions snapshot.
func Positions(p *investapi.PositionsResponse) PositionsView {
	money := moneyValues(p.GetMoney())
	blockedMoney := moneyValues(p.GetBlocked())
	securities := make([]SecurityPositionView, 0, len(p.GetSecurities()))
	for _, position := range p.GetSecurities() {
		securities = append(securities, SecurityPositionView{
			InstrumentUID: position.GetInstrumentUid(), PositionUID: position.GetPositionUid(),
			FIGI: position.GetFigi(), Ticker: position.GetTicker(), ClassCode: position.GetClassCode(),
			InstrumentType: position.GetInstrumentType(), Balance: strconv.FormatInt(position.GetBalance(), 10),
			Blocked: strconv.FormatInt(position.GetBlocked(), 10), ExchangeBlocked: position.GetExchangeBlocked(),
		})
	}
	futures := make([]DerivativePositionView, 0, len(p.GetFutures()))
	for _, position := range p.GetFutures() {
		futures = append(futures, DerivativePositionView{
			InstrumentUID: position.GetInstrumentUid(), PositionUID: position.GetPositionUid(), FIGI: position.GetFigi(),
			Ticker: position.GetTicker(), ClassCode: position.GetClassCode(), Balance: strconv.FormatInt(position.GetBalance(), 10),
			Blocked: strconv.FormatInt(position.GetBlocked(), 10),
		})
	}
	options := make([]DerivativePositionView, 0, len(p.GetOptions()))
	for _, position := range p.GetOptions() {
		options = append(options, DerivativePositionView{
			InstrumentUID: position.GetInstrumentUid(), PositionUID: position.GetPositionUid(),
			Ticker: position.GetTicker(), ClassCode: position.GetClassCode(), Balance: strconv.FormatInt(position.GetBalance(), 10),
			Blocked: strconv.FormatInt(position.GetBlocked(), 10),
		})
	}
	return PositionsView{
		AccountID: p.GetAccountId(), Money: money, BlockedMoney: blockedMoney,
		Securities: securities, Futures: futures, Options: options,
		LimitsLoadingInProgress: p.GetLimitsLoadingInProgress(),
	}
}

func moneyValues(values []*investapi.MoneyValue) []Decimal {
	result := make([]Decimal, 0, len(values))
	for _, value := range values {
		result = append(result, Money(value))
	}
	return result
}

// PositionsTable renders every money and instrument position as one row.
func PositionsTable(w io.Writer, view PositionsView) error {
	rows := make([][]string, 0, len(view.Money)+len(view.BlockedMoney)+len(view.Securities)+len(view.Futures)+len(view.Options))
	for _, money := range view.Money {
		rows = append(rows, []string{"money", money.Currency, "", money.Value, ""})
	}
	for _, money := range view.BlockedMoney {
		rows = append(rows, []string{"blocked_money", money.Currency, "", "", money.Value})
	}
	for _, position := range view.Securities {
		rows = append(rows, []string{"security", position.InstrumentUID, position.Ticker, position.Balance, position.Blocked})
	}
	for _, position := range view.Futures {
		rows = append(rows, []string{"future", position.InstrumentUID, position.Ticker, position.Balance, position.Blocked})
	}
	for _, position := range view.Options {
		rows = append(rows, []string{"option", position.InstrumentUID, position.Ticker, position.Balance, position.Blocked})
	}
	return Table(w, []string{"TYPE", "ID", "TICKER", "BALANCE", "BLOCKED"}, rows)
}

// BalanceCurrencyView combines all withdraw-limit categories for one currency.
type BalanceCurrencyView struct {
	Currency         string  `json:"currency"`
	Available        Decimal `json:"available"`
	Blocked          Decimal `json:"blocked"`
	BlockedGuarantee Decimal `json:"blocked_guarantee"`
}

// BalanceView is a per-currency summary of GetWithdrawLimits.
type BalanceView struct {
	Currencies []BalanceCurrencyView `json:"currencies"`
}

type moneySummary struct {
	available        *investapi.MoneyValue
	blocked          *investapi.MoneyValue
	blockedGuarantee *investapi.MoneyValue
}

// Balance groups withdraw limits by currency and sums duplicate entries.
func Balance(limits *investapi.WithdrawLimitsResponse) BalanceView {
	byCurrency := make(map[string]*moneySummary)
	add := func(values []*investapi.MoneyValue, selectValue func(*moneySummary) **investapi.MoneyValue) {
		for _, value := range values {
			summary := byCurrency[value.GetCurrency()]
			if summary == nil {
				summary = &moneySummary{}
				byCurrency[value.GetCurrency()] = summary
			}
			target := selectValue(summary)
			*target = addMoney(*target, value)
		}
	}
	add(limits.GetMoney(), func(summary *moneySummary) **investapi.MoneyValue { return &summary.available })
	add(limits.GetBlocked(), func(summary *moneySummary) **investapi.MoneyValue { return &summary.blocked })
	add(limits.GetBlockedGuarantee(), func(summary *moneySummary) **investapi.MoneyValue { return &summary.blockedGuarantee })

	currencies := make([]string, 0, len(byCurrency))
	for currency := range byCurrency {
		currencies = append(currencies, currency)
	}
	sort.Strings(currencies)
	views := make([]BalanceCurrencyView, 0, len(currencies))
	for _, currency := range currencies {
		summary := byCurrency[currency]
		views = append(views, BalanceCurrencyView{
			Currency:         currency,
			Available:        Money(withCurrency(summary.available, currency)),
			Blocked:          Money(withCurrency(summary.blocked, currency)),
			BlockedGuarantee: Money(withCurrency(summary.blockedGuarantee, currency)),
		})
	}
	return BalanceView{Currencies: views}
}

func addMoney(left, right *investapi.MoneyValue) *investapi.MoneyValue {
	if left == nil {
		left = &investapi.MoneyValue{Currency: right.GetCurrency()}
	}
	units := left.GetUnits() + right.GetUnits()
	nano := int64(left.GetNano()) + int64(right.GetNano())
	units += nano / 1_000_000_000
	nano %= 1_000_000_000
	if units > 0 && nano < 0 {
		units--
		nano += 1_000_000_000
	} else if units < 0 && nano > 0 {
		units++
		nano -= 1_000_000_000
	}
	return &investapi.MoneyValue{Currency: right.GetCurrency(), Units: units, Nano: int32(nano)}
}

func withCurrency(value *investapi.MoneyValue, currency string) *investapi.MoneyValue {
	if value != nil {
		return value
	}
	return &investapi.MoneyValue{Currency: currency}
}

// BalanceTable renders one row per currency.
func BalanceTable(w io.Writer, view BalanceView) error {
	rows := make([][]string, 0, len(view.Currencies))
	for _, currency := range view.Currencies {
		rows = append(rows, []string{currency.Currency, currency.Available.Value, currency.Blocked.Value, currency.BlockedGuarantee.Value})
	}
	return Table(w, []string{"CURRENCY", "AVAILABLE", "BLOCKED", "GUARANTEE"}, rows)
}
