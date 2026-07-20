package render

import (
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

func TestMarketDataStreamCandleUsesStableNumericAndTimeShapes(t *testing.T) {
	response := &investapi.MarketDataResponse{Payload: &investapi.MarketDataResponse_Candle{Candle: &investapi.Candle{
		InstrumentUid: "uid-1", Interval: investapi.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE,
		Open: &investapi.Quotation{Units: 12, Nano: 500_000_000}, Volume: 9007199254740993,
	}}}
	event := MarketDataStreamEvent(response, time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC))
	if event.Type != "candle" || event.SchemaVersion != SchemaVersion {
		t.Fatalf("event = %+v", event)
	}
	view, ok := event.Data.(StreamCandleView)
	if !ok {
		t.Fatalf("data type = %T", event.Data)
	}
	if view.Volume != "9007199254740993" || view.Open.Value != "12.5" || view.Interval != "SUBSCRIPTION_INTERVAL_ONE_MINUTE" {
		t.Fatalf("view = %+v", view)
	}
}

func TestOrdersStreamTradeUsesStringQuantityAndNormalizedPrice(t *testing.T) {
	response := &investapi.TradesStreamResponse{Payload: &investapi.TradesStreamResponse_OrderTrades{OrderTrades: &investapi.OrderTrades{
		OrderId: "order-1", AccountId: "account-1", Trades: []*investapi.OrderTrade{{
			TradeId: "trade-1", Quantity: 9007199254740993, Price: &investapi.Quotation{Units: 99, Nano: 250_000_000},
		}},
	}}}
	event := OrdersStreamEvent(response, time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC))
	if event.Type != "order_trade" || event.AccountID != "account-1" {
		t.Fatalf("event = %+v", event)
	}
	view, ok := event.Data.(OrderTradesView)
	if !ok || len(view.Trades) != 1 {
		t.Fatalf("data = %#v", event.Data)
	}
	if view.Trades[0].Quantity != "9007199254740993" || view.Trades[0].Price.Value != "99.25" {
		t.Fatalf("trade = %+v", view.Trades[0])
	}
}

func TestEmptyMarketDataResponseIsTypedControl(t *testing.T) {
	event := MarketDataStreamEvent(&investapi.MarketDataResponse{}, time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC))
	if event.Type != "control" {
		t.Fatalf("event type = %q, want control", event.Type)
	}
	view, ok := event.Data.(ControlStreamView)
	if !ok {
		t.Fatalf("data type = %T, want ControlStreamView", event.Data)
	}
	if view.Kind != "empty_response" || view.ProtobufOneofCase != "none" {
		t.Fatalf("view = %+v", view)
	}
}

func TestUnknownMarketDataFrameNamesUnrecognizedProtobufField(t *testing.T) {
	response := &investapi.MarketDataResponse{}
	unknown := protowire.AppendTag(nil, 99, protowire.BytesType)
	response.ProtoReflect().SetUnknown(protowire.AppendBytes(unknown, nil))
	event := MarketDataStreamEvent(response, time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC))
	if event.Type != "unknown" {
		t.Fatalf("event type = %q, want unknown", event.Type)
	}
	view, ok := event.Data.(UnknownStreamView)
	if !ok {
		t.Fatalf("data type = %T, want UnknownStreamView", event.Data)
	}
	if view.ProtobufOneofCase != "unknown_field_99" {
		t.Fatalf("protobuf oneof case = %q, want unknown_field_99", view.ProtobufOneofCase)
	}
}

func TestNilMarketDataFrameCarriesUnknownCaseData(t *testing.T) {
	event := MarketDataStreamEvent(nil, time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC))
	if event.Type != "unknown" {
		t.Fatalf("event type = %q, want unknown", event.Type)
	}
	view, ok := event.Data.(UnknownStreamView)
	if !ok || view.ProtobufOneofCase != "nil_message" {
		t.Fatalf("data = %#v, want nil_message case", event.Data)
	}
}
