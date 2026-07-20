package render

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

func TestNewsViewsMapNestedFieldsAndStableScalarFormats(t *testing.T) {
	summary := "Short summary"
	timestamp := timestamppb.New(time.Date(2026, 7, 19, 8, 30, 0, 0, time.FixedZone("UTC+4", 4*60*60)))
	views := News([]*investapi.NewsItem{{
		Id: 42, Source: "T-Bank", Title: "Title", Content: "Body", Summary: &summary,
		Tables: []*investapi.Table{{Table: "A|B"}}, Priority: true, Ts: timestamp,
		InstrumentId: []*investapi.NewsInstrument{{Instrument: &investapi.NewsInstrumentInfo{
			InstrumentUid: "uid-1", Ticker: "SBER", ClassCode: "TQBR",
		}}},
	}})
	if len(views) != 1 || views[0].ID != "42" || views[0].Summary == nil || *views[0].Summary != summary {
		t.Fatalf("views = %+v", views)
	}
	if views[0].Time != "2026-07-19T04:30:00Z" || len(views[0].Tables) != 1 || views[0].Tables[0].Table != "A|B" {
		t.Errorf("news = %+v", views[0])
	}
	if len(views[0].Instruments) != 1 || views[0].Instruments[0].InstrumentUID != "uid-1" {
		t.Errorf("instruments = %+v", views[0].Instruments)
	}
}

func TestFundamentalViewsMapAllFieldFamilies(t *testing.T) {
	date := timestamppb.New(time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC))
	views := Fundamentals([]*investapi.GetAssetFundamentalsResponse_StatisticResponse{{
		AssetUid: "asset-1", Currency: "rub", MarketCapitalization: 1000, HighPriceLast_52Weeks: 310.5,
		RevenueTtm: 500, PeRatioTtm: 7.5, TotalDebtMrq: 200, DividendYieldDailyTtm: 8.2,
		DomicileIndicatorCode: "RU", NumberOfEmployees: 1234, ExDividendDate: date,
		FiscalPeriodStartDate: date, FiscalPeriodEndDate: date, RevenueChangeFiveYears: 12.3,
		EpsChangeFiveYears: 4.5, EbitdaChangeFiveYears: 6.7, TotalDebtChangeFiveYears: -2.5, EvToSales: 1.2,
	}})
	if len(views) != 1 || views[0].AssetUID != "asset-1" || views[0].MarketCapitalization != 1000 {
		t.Fatalf("views = %+v", views)
	}
	if views[0].ExDividendDate != "2026-07-19T00:00:00Z" || views[0].EVToSales != 1.2 || views[0].TotalDebtChangeFiveYears != -2.5 {
		t.Errorf("fundamental = %+v", views[0])
	}
	payload, err := json.Marshal(views[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"market_capitalization"`, `"fiscal_period_end_date"`, `"ev_to_sales"`} {
		if !bytes.Contains(payload, []byte(field)) {
			t.Errorf("JSON %s missing %s", payload, field)
		}
	}
}

func TestFundamentalViewsMapEveryProtoBusinessField(t *testing.T) {
	date := timestamppb.New(time.Date(2026, 7, 19, 5, 6, 7, 0, time.UTC))
	item := &investapi.GetAssetFundamentalsResponse_StatisticResponse{}
	protoValue := reflect.ValueOf(item).Elem()
	protoType := protoValue.Type()
	for i := 0; i < protoType.NumField(); i++ {
		field := protoType.Field(i)
		if field.PkgPath != "" || jsonFieldName(field) == "" {
			continue
		}
		switch value := protoValue.Field(i); value.Kind() {
		case reflect.String:
			value.SetString("value-" + jsonFieldName(field))
		case reflect.Float64:
			value.SetFloat(float64(i) + 0.25)
		case reflect.Pointer:
			if value.Type() != reflect.TypeOf(date) {
				t.Fatalf("unsupported proto pointer field %s: %s", field.Name, value.Type())
			}
			value.Set(reflect.ValueOf(date))
		default:
			t.Fatalf("unsupported proto field %s: %s", field.Name, value.Kind())
		}
	}

	view := Fundamentals([]*investapi.GetAssetFundamentalsResponse_StatisticResponse{item})[0]
	viewValue := reflect.ValueOf(view)
	viewFields := businessJSONFields(view)
	protoFields := businessJSONFields(item)
	if len(viewFields) != len(protoFields) {
		t.Fatalf("view fields = %d, proto business fields = %d", len(viewFields), len(protoFields))
	}
	for name, protoField := range protoFields {
		viewField, ok := viewFields[name]
		if !ok {
			t.Errorf("view missing proto field %q", name)
			continue
		}
		got := viewValue.FieldByIndex(viewField.Index).Interface()
		source := protoValue.FieldByIndex(protoField.Index)
		var want any
		switch source.Kind() {
		case reflect.String, reflect.Float64:
			want = source.Interface()
		case reflect.Pointer:
			want = Timestamp(source.Interface().(*timestamppb.Timestamp))
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("field %s = %#v, want %#v", name, got, want)
		}
	}
}

func TestResearchViewSchemasDoNotSkipProtoBusinessFields(t *testing.T) {
	tests := []struct {
		name  string
		proto any
		view  any
	}{
		{"fundamentals", &investapi.GetAssetFundamentalsResponse_StatisticResponse{}, FundamentalView{}},
		{"forecast target", &investapi.GetForecastResponse_TargetItem{}, ForecastTargetView{}},
		{"forecast consensus", &investapi.GetForecastResponse_ConsensusItem{}, ForecastConsensusView{}},
		{"consensus forecast", &investapi.GetConsensusForecastsResponse_ConsensusForecastsItem{}, ConsensusForecastView{}},
		{"consensus page", &investapi.PageResponse{}, ResearchPageView{}},
		{"insider deal", &investapi.GetInsiderDealsResponse_InsiderDeal{}, InsiderDealView{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			protoFields := businessJSONFields(test.proto)
			viewFields := businessJSONFields(test.view)
			for name := range protoFields {
				if _, ok := viewFields[name]; !ok {
					t.Errorf("view missing proto field %q", name)
				}
			}
			for name := range viewFields {
				if _, ok := protoFields[name]; !ok {
					t.Errorf("view field %q has no proto source", name)
				}
			}
		})
	}
}

func businessJSONFields(value any) map[string]reflect.StructField {
	valueType := reflect.TypeOf(value)
	if valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	fields := make(map[string]reflect.StructField)
	for i := 0; i < valueType.NumField(); i++ {
		field := valueType.Field(i)
		name := jsonFieldName(field)
		if field.PkgPath == "" && name != "" {
			fields[name] = field
		}
	}
	return fields
}

func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" || tag == "-" {
		return ""
	}
	return strings.Split(tag, ",")[0]
}

func TestForecastViewsUseDecimalTimestampAndEnumConventions(t *testing.T) {
	date := timestamppb.New(time.Date(2026, 7, 19, 5, 6, 7, 0, time.UTC))
	targets := ForecastTargets([]*investapi.GetForecastResponse_TargetItem{{
		Uid: "uid-1", Ticker: "SBER", Company: "Invest House",
		Recommendation: investapi.Recommendation_RECOMMENDATION_BUY, RecommendationDate: date, Currency: "rub",
		CurrentPrice: &investapi.Quotation{Units: 250, Nano: 500_000_000},
		TargetPrice:  &investapi.Quotation{Units: 300}, PriceChange: &investapi.Quotation{Units: 49, Nano: 500_000_000},
		PriceChangeRel: &investapi.Quotation{Units: 19, Nano: 760_000_000}, ShowName: "Sber",
	}})
	if len(targets) != 1 || targets[0].Recommendation != "RECOMMENDATION_BUY" || targets[0].CurrentPrice.Value != "250.5" {
		t.Fatalf("targets = %+v", targets)
	}
	if targets[0].RecommendationDate != "2026-07-19T05:06:07Z" || targets[0].TargetPrice.Value != "300" {
		t.Errorf("target = %+v", targets[0])
	}

	consensus := ForecastConsensus(&investapi.GetForecastResponse_ConsensusItem{
		Uid: "uid-1", Ticker: "SBER", Recommendation: investapi.Recommendation_RECOMMENDATION_HOLD, Currency: "rub",
		CurrentPrice: &investapi.Quotation{Units: 250}, Consensus: &investapi.Quotation{Units: 280},
		MinTarget: &investapi.Quotation{Units: 220}, MaxTarget: &investapi.Quotation{Units: 330},
		PriceChange: &investapi.Quotation{Units: 30}, PriceChangeRel: &investapi.Quotation{Units: 12},
	})
	if consensus == nil || consensus.Recommendation != "RECOMMENDATION_HOLD" || consensus.Consensus.Value != "280" {
		t.Fatalf("consensus = %+v", consensus)
	}
	if ForecastConsensus(nil) != nil {
		t.Fatal("nil protobuf consensus must remain nil")
	}
}

func TestConsensusForecastAndPageViewsMapEveryField(t *testing.T) {
	date := timestamppb.New(time.Date(2026, 7, 19, 5, 6, 7, 0, time.UTC))
	views := ConsensusForecasts([]*investapi.GetConsensusForecastsResponse_ConsensusForecastsItem{{
		Uid: "uid-1", AssetUid: "asset-1", CreatedAt: date,
		BestTargetPrice: &investapi.Quotation{Units: 300}, BestTargetLow: &investapi.Quotation{Units: 250},
		BestTargetHigh: &investapi.Quotation{Units: 350}, TotalBuyRecommend: 5, TotalHoldRecommend: 2,
		TotalSellRecommend: 1, Currency: "rub", Consensus: investapi.Recommendation_RECOMMENDATION_BUY, PrognosisDate: date,
	}})
	if len(views) != 1 || views[0].AssetUID != "asset-1" || views[0].BestTargetHigh.Value != "350" {
		t.Fatalf("views = %+v", views)
	}
	if views[0].Consensus != "RECOMMENDATION_BUY" || views[0].PrognosisDate != "2026-07-19T05:06:07Z" {
		t.Errorf("view = %+v", views[0])
	}
	page := ResearchPage(&investapi.PageResponse{Limit: 25, PageNumber: 3, TotalCount: 80})
	if page.Limit != 25 || page.PageNumber != 3 || page.TotalCount != 80 {
		t.Errorf("page = %+v", page)
	}
}

func TestInsiderDealViewsUseStringInt64DecimalTimestampAndEnum(t *testing.T) {
	date := timestamppb.New(time.Date(2026, 7, 19, 5, 6, 7, 0, time.UTC))
	views := InsiderDeals([]*investapi.GetInsiderDealsResponse_InsiderDeal{{
		TradeId: 9007199254740993, Direction: investapi.GetInsiderDealsResponse_TRADE_DIRECTION_INCREASE,
		Currency: "rub", Date: date, Quantity: 1234567890123, Price: &investapi.Quotation{Units: 250, Nano: 500_000_000},
		InstrumentUid: "uid-1", Ticker: "SBER", InvestorName: "Investor", InvestorPosition: "director",
		Percentage: 1.25, IsOptionExecution: true, DisclosureDate: date,
	}})
	if len(views) != 1 || views[0].TradeID != "9007199254740993" || views[0].Quantity != "1234567890123" {
		t.Fatalf("views = %+v", views)
	}
	if views[0].Direction != "TRADE_DIRECTION_INCREASE" || views[0].Price.Value != "250.5" || views[0].DisclosureDate != "2026-07-19T05:06:07Z" {
		t.Errorf("deal = %+v", views[0])
	}
}

func TestResearchTablesExposeReadableRowsAndPagination(t *testing.T) {
	var output bytes.Buffer
	checks := []struct {
		name   string
		marker string
		render func() error
	}{
		{"news", "TITLE", func() error { return NewsTable(&output, []NewsView{{ID: "1", Title: "Title"}}) }},
		{"fundamentals", "MARKET_CAP", func() error { return FundamentalsTable(&output, []FundamentalView{{AssetUID: "asset"}}) }},
		{"forecast targets", "COMPANY", func() error { return ForecastTargetsTable(&output, []ForecastTargetView{{UID: "uid"}}) }},
		{"forecast consensus", "CONSENSUS", func() error { return ForecastConsensusTable(&output, &ForecastConsensusView{UID: "uid"}) }},
		{"consensus list", "BUY", func() error { return ConsensusForecastsTable(&output, []ConsensusForecastView{{UID: "uid"}}) }},
		{"page", "PAGE_NUMBER", func() error { return ResearchPageTable(&output, ResearchPageView{PageNumber: 2}) }},
		{"insider deals", "INVESTOR", func() error { return InsiderDealsTable(&output, []InsiderDealView{{TradeID: "7"}}) }},
	}
	for _, check := range checks {
		output.Reset()
		if err := check.render(); err != nil {
			t.Fatalf("%s: %v", check.name, err)
		}
		if !strings.Contains(output.String(), check.marker) {
			t.Errorf("%s table = %q, want %q", check.name, output.String(), check.marker)
		}
	}
}
