package render

import (
	"io"
	"strconv"
	"strings"

	investapi "tinvest/internal/pb/investapi"
)

// UnaryLimitView is one group of tariff limits for unary methods.
type UnaryLimitView struct {
	LimitPerMinute int32    `json:"limit_per_minute"`
	LimitPerSecond *int32   `json:"limit_per_second,omitempty"`
	Methods        []string `json:"methods"`
}

// StreamLimitView is one group of stream connection limits.
type StreamLimitView struct {
	Limit   int32    `json:"limit"`
	Open    int32    `json:"open"`
	Streams []string `json:"streams"`
}

// TariffView is the structured GetUserTariff response.
type TariffView struct {
	UnaryLimits  []UnaryLimitView  `json:"unary_limits"`
	StreamLimits []StreamLimitView `json:"stream_limits"`
}

// Tariff converts user tariff limits.
func Tariff(response *investapi.GetUserTariffResponse) TariffView {
	unary := make([]UnaryLimitView, 0, len(response.GetUnaryLimits()))
	for _, limit := range response.GetUnaryLimits() {
		unary = append(unary, UnaryLimitView{
			LimitPerMinute: limit.GetLimitPerMinute(), LimitPerSecond: limit.LimitPerSecond, Methods: limit.GetMethods(),
		})
	}
	streams := make([]StreamLimitView, 0, len(response.GetStreamLimits()))
	for _, limit := range response.GetStreamLimits() {
		streams = append(streams, StreamLimitView{Limit: limit.GetLimit(), Open: limit.GetOpen(), Streams: limit.GetStreams()})
	}
	return TariffView{UnaryLimits: unary, StreamLimits: streams}
}

// TariffTable renders unary and stream limits as structured rows.
func TariffTable(w io.Writer, view TariffView) error {
	rows := make([][]string, 0, len(view.UnaryLimits)+len(view.StreamLimits))
	for _, limit := range view.UnaryLimits {
		perSecond := ""
		if limit.LimitPerSecond != nil {
			perSecond = strconv.Itoa(int(*limit.LimitPerSecond))
		}
		rows = append(rows, []string{"unary", strconv.Itoa(int(limit.LimitPerMinute)), perSecond, "", strings.Join(limit.Methods, ",")})
	}
	for _, limit := range view.StreamLimits {
		rows = append(rows, []string{"stream", strconv.Itoa(int(limit.Limit)), "", strconv.Itoa(int(limit.Open)), strings.Join(limit.Streams, ",")})
	}
	return Table(w, []string{"TYPE", "LIMIT", "PER_SECOND", "OPEN", "METHODS/STREAMS"}, rows)
}

// MarginView is the stable shape of account margin attributes.
type MarginView struct {
	LiquidPortfolio       Decimal `json:"liquid_portfolio"`
	StartingMargin        Decimal `json:"starting_margin"`
	MinimalMargin         Decimal `json:"minimal_margin"`
	FundsSufficiencyLevel Decimal `json:"funds_sufficiency_level"`
	AmountOfMissingFunds  Decimal `json:"amount_of_missing_funds"`
	CorrectedMargin       Decimal `json:"corrected_margin"`
	GuaranteeForFutures   Decimal `json:"guarantee_for_futures"`
}

// Margin converts account margin attributes.
func Margin(response *investapi.GetMarginAttributesResponse) MarginView {
	return MarginView{
		LiquidPortfolio: Money(response.GetLiquidPortfolio()), StartingMargin: Money(response.GetStartingMargin()),
		MinimalMargin: Money(response.GetMinimalMargin()), FundsSufficiencyLevel: Quotation(response.GetFundsSufficiencyLevel()),
		AmountOfMissingFunds: Money(response.GetAmountOfMissingFunds()), CorrectedMargin: Money(response.GetCorrectedMargin()),
		GuaranteeForFutures: Money(response.GetGuaranteeForFutures()),
	}
}

// MarginTable renders account margin attributes as key/value rows.
func MarginTable(w io.Writer, view MarginView) error {
	return Table(w, []string{"FIELD", "VALUE", "CURRENCY"}, [][]string{
		{"liquid_portfolio", view.LiquidPortfolio.Value, view.LiquidPortfolio.Currency},
		{"starting_margin", view.StartingMargin.Value, view.StartingMargin.Currency},
		{"minimal_margin", view.MinimalMargin.Value, view.MinimalMargin.Currency},
		{"funds_sufficiency_level", view.FundsSufficiencyLevel.Value, ""},
		{"amount_of_missing_funds", view.AmountOfMissingFunds.Value, view.AmountOfMissingFunds.Currency},
		{"corrected_margin", view.CorrectedMargin.Value, view.CorrectedMargin.Currency},
		{"guarantee_for_futures", view.GuaranteeForFutures.Value, view.GuaranteeForFutures.Currency},
	})
}
