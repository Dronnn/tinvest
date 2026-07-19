package render

import (
	"io"
	"strconv"

	investapi "tinvest/internal/pb/investapi"
)

// NewsTableView preserves one table embedded in a news item.
type NewsTableView struct {
	Table string `json:"table"`
}

// NewsInstrumentView identifies an instrument referenced by a news item.
type NewsInstrumentView struct {
	InstrumentUID string `json:"instrument_uid"`
	Ticker        string `json:"ticker"`
	ClassCode     string `json:"class_code"`
}

// NewsView is one current-news item.
type NewsView struct {
	ID          string               `json:"id"`
	Source      string               `json:"source"`
	Title       string               `json:"title"`
	Content     string               `json:"content"`
	Summary     *string              `json:"summary,omitempty"`
	Tables      []NewsTableView      `json:"tables"`
	Instruments []NewsInstrumentView `json:"instruments"`
	Priority    bool                 `json:"priority"`
	Time        string               `json:"time,omitempty"`
}

// News converts news items, preserving broker order and nested records.
func News(items []*investapi.NewsItem) []NewsView {
	views := make([]NewsView, 0, len(items))
	for _, item := range items {
		tables := make([]NewsTableView, 0, len(item.GetTables()))
		for _, table := range item.GetTables() {
			tables = append(tables, NewsTableView{Table: table.GetTable()})
		}
		instruments := make([]NewsInstrumentView, 0, len(item.GetInstrumentId()))
		for _, reference := range item.GetInstrumentId() {
			instrument := reference.GetInstrument()
			instruments = append(instruments, NewsInstrumentView{
				InstrumentUID: instrument.GetInstrumentUid(), Ticker: instrument.GetTicker(), ClassCode: instrument.GetClassCode(),
			})
		}
		views = append(views, NewsView{
			ID: strconv.FormatInt(item.GetId(), 10), Source: item.GetSource(), Title: item.GetTitle(), Content: item.GetContent(),
			Summary: item.Summary, Tables: tables, Instruments: instruments, Priority: item.GetPriority(), Time: Timestamp(item.GetTs()),
		})
	}
	return views
}

// NewsTable renders a compact current-news list.
func NewsTable(w io.Writer, views []NewsView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.Time, view.ID, view.Source, view.Title, strconv.FormatBool(view.Priority)})
	}
	return Table(w, []string{"TIME", "ID", "SOURCE", "TITLE", "PRIORITY"}, rows)
}

// FundamentalView contains every field from StatisticResponse. Native double
// fields remain numbers; the three timestamps use the CLI's UTC convention.
type FundamentalView struct {
	AssetUID                         string  `json:"asset_uid"`
	Currency                         string  `json:"currency"`
	MarketCapitalization             float64 `json:"market_capitalization"`
	HighPriceLast52Weeks             float64 `json:"high_price_last_52_weeks"`
	LowPriceLast52Weeks              float64 `json:"low_price_last_52_weeks"`
	AverageDailyVolumeLast10Days     float64 `json:"average_daily_volume_last_10_days"`
	AverageDailyVolumeLast4Weeks     float64 `json:"average_daily_volume_last_4_weeks"`
	Beta                             float64 `json:"beta"`
	FreeFloat                        float64 `json:"free_float"`
	ForwardAnnualDividendYield       float64 `json:"forward_annual_dividend_yield"`
	SharesOutstanding                float64 `json:"shares_outstanding"`
	RevenueTTM                       float64 `json:"revenue_ttm"`
	EBITDATTM                        float64 `json:"ebitda_ttm"`
	NetIncomeTTM                     float64 `json:"net_income_ttm"`
	EPSTTM                           float64 `json:"eps_ttm"`
	DilutedEPSTTM                    float64 `json:"diluted_eps_ttm"`
	FreeCashFlowTTM                  float64 `json:"free_cash_flow_ttm"`
	FiveYearAnnualRevenueGrowthRate  float64 `json:"five_year_annual_revenue_growth_rate"`
	ThreeYearAnnualRevenueGrowthRate float64 `json:"three_year_annual_revenue_growth_rate"`
	PERatioTTM                       float64 `json:"pe_ratio_ttm"`
	PriceToSalesTTM                  float64 `json:"price_to_sales_ttm"`
	PriceToBookTTM                   float64 `json:"price_to_book_ttm"`
	PriceToFreeCashFlowTTM           float64 `json:"price_to_free_cash_flow_ttm"`
	TotalEnterpriseValueMRQ          float64 `json:"total_enterprise_value_mrq"`
	EVToEBITDAMRQ                    float64 `json:"ev_to_ebitda_mrq"`
	NetMarginMRQ                     float64 `json:"net_margin_mrq"`
	NetInterestMarginMRQ             float64 `json:"net_interest_margin_mrq"`
	ROE                              float64 `json:"roe"`
	ROA                              float64 `json:"roa"`
	ROIC                             float64 `json:"roic"`
	TotalDebtMRQ                     float64 `json:"total_debt_mrq"`
	TotalDebtToEquityMRQ             float64 `json:"total_debt_to_equity_mrq"`
	TotalDebtToEBITDAMRQ             float64 `json:"total_debt_to_ebitda_mrq"`
	FreeCashFlowToPrice              float64 `json:"free_cash_flow_to_price"`
	NetDebtToEBITDA                  float64 `json:"net_debt_to_ebitda"`
	CurrentRatioMRQ                  float64 `json:"current_ratio_mrq"`
	FixedChargeCoverageRatioFY       float64 `json:"fixed_charge_coverage_ratio_fy"`
	DividendYieldDailyTTM            float64 `json:"dividend_yield_daily_ttm"`
	DividendRateTTM                  float64 `json:"dividend_rate_ttm"`
	DividendsPerShare                float64 `json:"dividends_per_share"`
	FiveYearsAverageDividendYield    float64 `json:"five_years_average_dividend_yield"`
	FiveYearAnnualDividendGrowthRate float64 `json:"five_year_annual_dividend_growth_rate"`
	DividendPayoutRatioFY            float64 `json:"dividend_payout_ratio_fy"`
	BuyBackTTM                       float64 `json:"buy_back_ttm"`
	OneYearAnnualRevenueGrowthRate   float64 `json:"one_year_annual_revenue_growth_rate"`
	DomicileIndicatorCode            string  `json:"domicile_indicator_code"`
	ADRToCommonShareRatio            float64 `json:"adr_to_common_share_ratio"`
	NumberOfEmployees                float64 `json:"number_of_employees"`
	ExDividendDate                   string  `json:"ex_dividend_date,omitempty"`
	FiscalPeriodStartDate            string  `json:"fiscal_period_start_date,omitempty"`
	FiscalPeriodEndDate              string  `json:"fiscal_period_end_date,omitempty"`
	RevenueChangeFiveYears           float64 `json:"revenue_change_five_years"`
	EPSChangeFiveYears               float64 `json:"eps_change_five_years"`
	EBITDAChangeFiveYears            float64 `json:"ebitda_change_five_years"`
	TotalDebtChangeFiveYears         float64 `json:"total_debt_change_five_years"`
	EVToSales                        float64 `json:"ev_to_sales"`
}

// Fundamentals converts fundamental statistics without dropping metrics.
func Fundamentals(items []*investapi.GetAssetFundamentalsResponse_StatisticResponse) []FundamentalView {
	views := make([]FundamentalView, 0, len(items))
	for _, item := range items {
		views = append(views, FundamentalView{
			AssetUID: item.GetAssetUid(), Currency: item.GetCurrency(), MarketCapitalization: item.GetMarketCapitalization(),
			HighPriceLast52Weeks: item.GetHighPriceLast_52Weeks(), LowPriceLast52Weeks: item.GetLowPriceLast_52Weeks(),
			AverageDailyVolumeLast10Days: item.GetAverageDailyVolumeLast_10Days(), AverageDailyVolumeLast4Weeks: item.GetAverageDailyVolumeLast_4Weeks(),
			Beta: item.GetBeta(), FreeFloat: item.GetFreeFloat(), ForwardAnnualDividendYield: item.GetForwardAnnualDividendYield(),
			SharesOutstanding: item.GetSharesOutstanding(), RevenueTTM: item.GetRevenueTtm(), EBITDATTM: item.GetEbitdaTtm(),
			NetIncomeTTM: item.GetNetIncomeTtm(), EPSTTM: item.GetEpsTtm(), DilutedEPSTTM: item.GetDilutedEpsTtm(),
			FreeCashFlowTTM: item.GetFreeCashFlowTtm(), FiveYearAnnualRevenueGrowthRate: item.GetFiveYearAnnualRevenueGrowthRate(),
			ThreeYearAnnualRevenueGrowthRate: item.GetThreeYearAnnualRevenueGrowthRate(), PERatioTTM: item.GetPeRatioTtm(),
			PriceToSalesTTM: item.GetPriceToSalesTtm(), PriceToBookTTM: item.GetPriceToBookTtm(),
			PriceToFreeCashFlowTTM: item.GetPriceToFreeCashFlowTtm(), TotalEnterpriseValueMRQ: item.GetTotalEnterpriseValueMrq(),
			EVToEBITDAMRQ: item.GetEvToEbitdaMrq(), NetMarginMRQ: item.GetNetMarginMrq(), NetInterestMarginMRQ: item.GetNetInterestMarginMrq(),
			ROE: item.GetRoe(), ROA: item.GetRoa(), ROIC: item.GetRoic(), TotalDebtMRQ: item.GetTotalDebtMrq(),
			TotalDebtToEquityMRQ: item.GetTotalDebtToEquityMrq(), TotalDebtToEBITDAMRQ: item.GetTotalDebtToEbitdaMrq(),
			FreeCashFlowToPrice: item.GetFreeCashFlowToPrice(), NetDebtToEBITDA: item.GetNetDebtToEbitda(),
			CurrentRatioMRQ: item.GetCurrentRatioMrq(), FixedChargeCoverageRatioFY: item.GetFixedChargeCoverageRatioFy(),
			DividendYieldDailyTTM: item.GetDividendYieldDailyTtm(), DividendRateTTM: item.GetDividendRateTtm(),
			DividendsPerShare: item.GetDividendsPerShare(), FiveYearsAverageDividendYield: item.GetFiveYearsAverageDividendYield(),
			FiveYearAnnualDividendGrowthRate: item.GetFiveYearAnnualDividendGrowthRate(), DividendPayoutRatioFY: item.GetDividendPayoutRatioFy(),
			BuyBackTTM: item.GetBuyBackTtm(), OneYearAnnualRevenueGrowthRate: item.GetOneYearAnnualRevenueGrowthRate(),
			DomicileIndicatorCode: item.GetDomicileIndicatorCode(), ADRToCommonShareRatio: item.GetAdrToCommonShareRatio(),
			NumberOfEmployees: item.GetNumberOfEmployees(), ExDividendDate: Timestamp(item.GetExDividendDate()),
			FiscalPeriodStartDate: Timestamp(item.GetFiscalPeriodStartDate()), FiscalPeriodEndDate: Timestamp(item.GetFiscalPeriodEndDate()),
			RevenueChangeFiveYears: item.GetRevenueChangeFiveYears(), EPSChangeFiveYears: item.GetEpsChangeFiveYears(),
			EBITDAChangeFiveYears: item.GetEbitdaChangeFiveYears(), TotalDebtChangeFiveYears: item.GetTotalDebtChangeFiveYears(),
			EVToSales: item.GetEvToSales(),
		})
	}
	return views
}

// FundamentalsTable renders the most useful comparison metrics; JSON retains
// the full fundamental record.
func FundamentalsTable(w io.Writer, views []FundamentalView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{
			view.AssetUID, view.Currency, float64String(view.MarketCapitalization), float64String(view.RevenueTTM),
			float64String(view.EBITDATTM), float64String(view.NetIncomeTTM), float64String(view.PERatioTTM),
			float64String(view.DividendYieldDailyTTM),
		})
	}
	return Table(w, []string{"ASSET_UID", "CURRENCY", "MARKET_CAP", "REVENUE_TTM", "EBITDA_TTM", "NET_INCOME_TTM", "PE_TTM", "DIVIDEND_YIELD_TTM"}, rows)
}

// ForecastTargetView is one investment-house target.
type ForecastTargetView struct {
	UID                 string  `json:"uid"`
	Ticker              string  `json:"ticker"`
	Company             string  `json:"company"`
	Recommendation      string  `json:"recommendation"`
	RecommendationDate  string  `json:"recommendation_date,omitempty"`
	Currency            string  `json:"currency"`
	CurrentPrice        Decimal `json:"current_price"`
	TargetPrice         Decimal `json:"target_price"`
	PriceChange         Decimal `json:"price_change"`
	PriceChangeRelative Decimal `json:"price_change_rel"`
	ShowName            string  `json:"show_name"`
}

// ForecastTargets converts investment-house forecasts.
func ForecastTargets(items []*investapi.GetForecastResponse_TargetItem) []ForecastTargetView {
	views := make([]ForecastTargetView, 0, len(items))
	for _, item := range items {
		views = append(views, ForecastTargetView{
			UID: item.GetUid(), Ticker: item.GetTicker(), Company: item.GetCompany(), Recommendation: item.GetRecommendation().String(),
			RecommendationDate: Timestamp(item.GetRecommendationDate()), Currency: item.GetCurrency(), CurrentPrice: Quotation(item.GetCurrentPrice()),
			TargetPrice: Quotation(item.GetTargetPrice()), PriceChange: Quotation(item.GetPriceChange()),
			PriceChangeRelative: Quotation(item.GetPriceChangeRel()), ShowName: item.GetShowName(),
		})
	}
	return views
}

// ForecastTargetsTable renders investment-house forecast rows.
func ForecastTargetsTable(w io.Writer, views []ForecastTargetView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.RecommendationDate, view.Ticker, view.Company, view.Recommendation, view.CurrentPrice.Value, view.TargetPrice.Value, view.Currency})
	}
	return Table(w, []string{"DATE", "TICKER", "COMPANY", "RECOMMENDATION", "CURRENT", "TARGET", "CURRENCY"}, rows)
}

// ForecastConsensusView is the consensus returned alongside forecast targets.
type ForecastConsensusView struct {
	UID                 string  `json:"uid"`
	Ticker              string  `json:"ticker"`
	Recommendation      string  `json:"recommendation"`
	Currency            string  `json:"currency"`
	CurrentPrice        Decimal `json:"current_price"`
	Consensus           Decimal `json:"consensus"`
	MinTarget           Decimal `json:"min_target"`
	MaxTarget           Decimal `json:"max_target"`
	PriceChange         Decimal `json:"price_change"`
	PriceChangeRelative Decimal `json:"price_change_rel"`
}

// ForecastConsensus converts the optional forecast consensus.
func ForecastConsensus(item *investapi.GetForecastResponse_ConsensusItem) *ForecastConsensusView {
	if item == nil {
		return nil
	}
	return &ForecastConsensusView{
		UID: item.GetUid(), Ticker: item.GetTicker(), Recommendation: item.GetRecommendation().String(), Currency: item.GetCurrency(),
		CurrentPrice: Quotation(item.GetCurrentPrice()), Consensus: Quotation(item.GetConsensus()), MinTarget: Quotation(item.GetMinTarget()),
		MaxTarget: Quotation(item.GetMaxTarget()), PriceChange: Quotation(item.GetPriceChange()), PriceChangeRelative: Quotation(item.GetPriceChangeRel()),
	}
}

// ForecastConsensusTable renders the optional consensus as one row.
func ForecastConsensusTable(w io.Writer, view *ForecastConsensusView) error {
	rows := make([][]string, 0, 1)
	if view != nil {
		rows = append(rows, []string{view.Ticker, view.Recommendation, view.CurrentPrice.Value, view.Consensus.Value, view.MinTarget.Value, view.MaxTarget.Value, view.Currency})
	}
	return Table(w, []string{"TICKER", "RECOMMENDATION", "CURRENT", "CONSENSUS", "MIN_TARGET", "MAX_TARGET", "CURRENCY"}, rows)
}

// ConsensusForecastView is one instrument-level consensus forecast.
type ConsensusForecastView struct {
	UID                string  `json:"uid"`
	AssetUID           string  `json:"asset_uid"`
	CreatedAt          string  `json:"created_at,omitempty"`
	BestTargetPrice    Decimal `json:"best_target_price"`
	BestTargetLow      Decimal `json:"best_target_low"`
	BestTargetHigh     Decimal `json:"best_target_high"`
	TotalBuyRecommend  int32   `json:"total_buy_recommend"`
	TotalHoldRecommend int32   `json:"total_hold_recommend"`
	TotalSellRecommend int32   `json:"total_sell_recommend"`
	Currency           string  `json:"currency"`
	Consensus          string  `json:"consensus"`
	PrognosisDate      string  `json:"prognosis_date,omitempty"`
}

// ConsensusForecasts converts one page of consensus forecasts.
func ConsensusForecasts(items []*investapi.GetConsensusForecastsResponse_ConsensusForecastsItem) []ConsensusForecastView {
	views := make([]ConsensusForecastView, 0, len(items))
	for _, item := range items {
		views = append(views, ConsensusForecastView{
			UID: item.GetUid(), AssetUID: item.GetAssetUid(), CreatedAt: Timestamp(item.GetCreatedAt()),
			BestTargetPrice: Quotation(item.GetBestTargetPrice()), BestTargetLow: Quotation(item.GetBestTargetLow()),
			BestTargetHigh: Quotation(item.GetBestTargetHigh()), TotalBuyRecommend: item.GetTotalBuyRecommend(),
			TotalHoldRecommend: item.GetTotalHoldRecommend(), TotalSellRecommend: item.GetTotalSellRecommend(),
			Currency: item.GetCurrency(), Consensus: item.GetConsensus().String(), PrognosisDate: Timestamp(item.GetPrognosisDate()),
		})
	}
	return views
}

// ConsensusForecastsTable renders one consensus page.
func ConsensusForecastsTable(w io.Writer, views []ConsensusForecastView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{
			view.PrognosisDate, view.UID, view.Consensus, view.BestTargetPrice.Value, view.BestTargetLow.Value,
			view.BestTargetHigh.Value, strconv.Itoa(int(view.TotalBuyRecommend)), strconv.Itoa(int(view.TotalHoldRecommend)),
			strconv.Itoa(int(view.TotalSellRecommend)), view.Currency,
		})
	}
	return Table(w, []string{"DATE", "UID", "CONSENSUS", "TARGET", "LOW", "HIGH", "BUY", "HOLD", "SELL", "CURRENCY"}, rows)
}

// ResearchPageView preserves the broker's page metadata.
type ResearchPageView struct {
	Limit      int32 `json:"limit"`
	PageNumber int32 `json:"page_number"`
	TotalCount int32 `json:"total_count"`
}

// ResearchPage converts page metadata.
func ResearchPage(page *investapi.PageResponse) ResearchPageView {
	return ResearchPageView{Limit: page.GetLimit(), PageNumber: page.GetPageNumber(), TotalCount: page.GetTotalCount()}
}

// ResearchPageTable emits page metadata after a table response.
func ResearchPageTable(w io.Writer, page ResearchPageView) error {
	return Table(w, []string{"PAGE_NUMBER", "LIMIT", "TOTAL_COUNT"}, [][]string{{
		strconv.Itoa(int(page.PageNumber)), strconv.Itoa(int(page.Limit)), strconv.Itoa(int(page.TotalCount)),
	}})
}

// InsiderDealView is one disclosed insider transaction.
type InsiderDealView struct {
	TradeID           string  `json:"trade_id"`
	Direction         string  `json:"direction"`
	Currency          string  `json:"currency"`
	Date              string  `json:"date,omitempty"`
	Quantity          string  `json:"quantity"`
	Price             Decimal `json:"price"`
	InstrumentUID     string  `json:"instrument_uid"`
	Ticker            string  `json:"ticker"`
	InvestorName      string  `json:"investor_name"`
	InvestorPosition  string  `json:"investor_position"`
	Percentage        float32 `json:"percentage"`
	IsOptionExecution bool    `json:"is_option_execution"`
	DisclosureDate    string  `json:"disclosure_date,omitempty"`
}

// InsiderDeals converts insider transactions using stable int64, decimal,
// timestamp, and enum representations.
func InsiderDeals(items []*investapi.GetInsiderDealsResponse_InsiderDeal) []InsiderDealView {
	views := make([]InsiderDealView, 0, len(items))
	for _, item := range items {
		views = append(views, InsiderDealView{
			TradeID: strconv.FormatInt(item.GetTradeId(), 10), Direction: item.GetDirection().String(), Currency: item.GetCurrency(),
			Date: Timestamp(item.GetDate()), Quantity: strconv.FormatInt(item.GetQuantity(), 10), Price: Quotation(item.GetPrice()),
			InstrumentUID: item.GetInstrumentUid(), Ticker: item.GetTicker(), InvestorName: item.GetInvestorName(),
			InvestorPosition: item.GetInvestorPosition(), Percentage: item.GetPercentage(), IsOptionExecution: item.GetIsOptionExecution(),
			DisclosureDate: Timestamp(item.GetDisclosureDate()),
		})
	}
	return views
}

// InsiderDealsTable renders disclosed insider transactions.
func InsiderDealsTable(w io.Writer, views []InsiderDealView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{
			view.Date, view.TradeID, view.Ticker, view.Direction, view.Quantity, view.Price.Value,
			view.Currency, view.InvestorName, view.InvestorPosition, float32String(view.Percentage),
		})
	}
	return Table(w, []string{"DATE", "TRADE_ID", "TICKER", "DIRECTION", "QUANTITY", "PRICE", "CURRENCY", "INVESTOR", "POSITION", "PERCENTAGE"}, rows)
}

func float64String(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func float32String(value float32) string {
	return strconv.FormatFloat(float64(value), 'g', -1, 32)
}
