package render

import (
	"encoding/json"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	streamrunner "github.com/Dronnn/tinvest/internal/stream"
	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

type LifecycleView struct {
	Attempt       int    `json:"attempt"`
	Subscriptions int    `json:"subscriptions"`
	Reason        string `json:"reason,omitempty"`
	Error         string `json:"error,omitempty"`
	Final         bool   `json:"final,omitempty"`
}

func LifecycleStreamEvent(lifecycle streamrunner.LifecycleEvent) StreamEvent {
	view := LifecycleView{
		Attempt: lifecycle.Attempt, Subscriptions: lifecycle.Subscriptions,
		Reason: lifecycle.Reason, Final: lifecycle.Final,
	}
	if lifecycle.Err != nil {
		view.Error = lifecycle.Err.Error()
	}
	return NewStreamEvent(string(lifecycle.Type), lifecycle.Time, view)
}

type PingView struct {
	ServerTime      string `json:"server_time,omitempty"`
	StreamID        string `json:"stream_id,omitempty"`
	PingRequestTime string `json:"ping_request_time,omitempty"`
}

func streamPing(ping *investapi.Ping) PingView {
	return PingView{
		ServerTime: Timestamp(ping.GetTime()), StreamID: ping.GetStreamId(),
		PingRequestTime: Timestamp(ping.GetPingRequestTime()),
	}
}

type StreamCandleView struct {
	InstrumentUID string  `json:"instrument_uid"`
	Ticker        string  `json:"ticker,omitempty"`
	ClassCode     string  `json:"class_code,omitempty"`
	FIGI          string  `json:"figi,omitempty"`
	Interval      string  `json:"interval"`
	Open          Decimal `json:"open"`
	High          Decimal `json:"high"`
	Low           Decimal `json:"low"`
	Close         Decimal `json:"close"`
	Volume        string  `json:"volume"`
	VolumeBuy     string  `json:"volume_buy"`
	VolumeSell    string  `json:"volume_sell"`
	CandleTime    string  `json:"candle_time,omitempty"`
	LastTradeTime string  `json:"last_trade_time,omitempty"`
	Source        string  `json:"source"`
}

type StreamTradeView struct {
	InstrumentUID string  `json:"instrument_uid"`
	Ticker        string  `json:"ticker,omitempty"`
	ClassCode     string  `json:"class_code,omitempty"`
	FIGI          string  `json:"figi,omitempty"`
	Direction     string  `json:"direction"`
	Price         Decimal `json:"price"`
	Quantity      string  `json:"quantity"`
	TradeTime     string  `json:"trade_time,omitempty"`
	Source        string  `json:"source"`
}

type StreamOrderBookView struct {
	InstrumentUID string               `json:"instrument_uid"`
	Ticker        string               `json:"ticker,omitempty"`
	ClassCode     string               `json:"class_code,omitempty"`
	FIGI          string               `json:"figi,omitempty"`
	Depth         int32                `json:"depth"`
	Consistent    bool                 `json:"is_consistent"`
	Bids          []OrderBookLevelView `json:"bids"`
	Asks          []OrderBookLevelView `json:"asks"`
	OrderbookTime string               `json:"orderbook_time,omitempty"`
	LimitUp       Decimal              `json:"limit_up"`
	LimitDown     Decimal              `json:"limit_down"`
	OrderBookType string               `json:"orderbook_type"`
}

type TradingStatusStreamView struct {
	InstrumentUID        string `json:"instrument_uid"`
	Ticker               string `json:"ticker,omitempty"`
	ClassCode            string `json:"class_code,omitempty"`
	FIGI                 string `json:"figi,omitempty"`
	Status               string `json:"status"`
	StatusTime           string `json:"status_time,omitempty"`
	LimitOrderAvailable  bool   `json:"limit_order_available"`
	MarketOrderAvailable bool   `json:"market_order_available"`
}

type OpenInterestStreamView struct {
	InstrumentUID string `json:"instrument_uid"`
	Ticker        string `json:"ticker,omitempty"`
	ClassCode     string `json:"class_code,omitempty"`
	Value         string `json:"value"`
	ValueTime     string `json:"value_time,omitempty"`
}

type ControlStreamView struct {
	Kind              string `json:"kind"`
	ProtobufOneofCase string `json:"protobuf_oneof_case"`
}

type UnknownStreamView struct {
	ProtobufOneofCase string `json:"protobuf_oneof_case"`
}

// MarketDataStreamEvent converts every MarketDataResponse oneof into one
// standalone, stable NDJSON frame.
func MarketDataStreamEvent(response *investapi.MarketDataResponse, at time.Time) StreamEvent {
	if response != nil && response.GetPayload() == nil && len(response.ProtoReflect().GetUnknown()) == 0 {
		return NewStreamEvent("control", at, ControlStreamView{Kind: "empty_response", ProtobufOneofCase: "none"})
	}
	switch payload := response.GetPayload().(type) {
	case *investapi.MarketDataResponse_Candle:
		value := payload.Candle
		return NewStreamEvent("candle", at, StreamCandleView{
			InstrumentUID: value.GetInstrumentUid(), Ticker: value.GetTicker(), ClassCode: value.GetClassCode(), FIGI: value.GetFigi(),
			Interval: value.GetInterval().String(), Open: Quotation(value.GetOpen()), High: Quotation(value.GetHigh()),
			Low: Quotation(value.GetLow()), Close: Quotation(value.GetClose()), Volume: strconv.FormatInt(value.GetVolume(), 10),
			VolumeBuy: strconv.FormatInt(value.GetVolumeBuy(), 10), VolumeSell: strconv.FormatInt(value.GetVolumeSell(), 10),
			CandleTime: Timestamp(value.GetTime()), LastTradeTime: Timestamp(value.GetLastTradeTs()), Source: value.GetCandleSourceType().String(),
		})
	case *investapi.MarketDataResponse_Trade:
		value := payload.Trade
		return NewStreamEvent("trade", at, StreamTradeView{
			InstrumentUID: value.GetInstrumentUid(), Ticker: value.GetTicker(), ClassCode: value.GetClassCode(), FIGI: value.GetFigi(),
			Direction: value.GetDirection().String(), Price: Quotation(value.GetPrice()), Quantity: strconv.FormatInt(value.GetQuantity(), 10),
			TradeTime: Timestamp(value.GetTime()), Source: value.GetTradeSource().String(),
		})
	case *investapi.MarketDataResponse_Orderbook:
		value := payload.Orderbook
		return NewStreamEvent("orderbook", at, StreamOrderBookView{
			InstrumentUID: value.GetInstrumentUid(), Ticker: value.GetTicker(), ClassCode: value.GetClassCode(), FIGI: value.GetFigi(),
			Depth: value.GetDepth(), Consistent: value.GetIsConsistent(), Bids: OrderBookLevels(value.GetBids()), Asks: OrderBookLevels(value.GetAsks()),
			OrderbookTime: Timestamp(value.GetTime()), LimitUp: Quotation(value.GetLimitUp()), LimitDown: Quotation(value.GetLimitDown()),
			OrderBookType: value.GetOrderBookType().String(),
		})
	case *investapi.MarketDataResponse_TradingStatus:
		value := payload.TradingStatus
		return NewStreamEvent("info", at, TradingStatusStreamView{
			InstrumentUID: value.GetInstrumentUid(), Ticker: value.GetTicker(), ClassCode: value.GetClassCode(), FIGI: value.GetFigi(),
			Status: value.GetTradingStatus().String(), StatusTime: Timestamp(value.GetTime()),
			LimitOrderAvailable: value.GetLimitOrderAvailableFlag(), MarketOrderAvailable: value.GetMarketOrderAvailableFlag(),
		})
	case *investapi.MarketDataResponse_LastPrice:
		return NewStreamEvent("last_price", at, LastPrice(payload.LastPrice))
	case *investapi.MarketDataResponse_OpenInterest:
		value := payload.OpenInterest
		return NewStreamEvent("open_interest", at, OpenInterestStreamView{
			InstrumentUID: value.GetInstrumentUid(), Ticker: value.GetTicker(), ClassCode: value.GetClassCode(),
			Value: strconv.FormatInt(value.GetOpenInterest(), 10), ValueTime: Timestamp(value.GetTime()),
		})
	case *investapi.MarketDataResponse_Ping:
		return NewStreamEvent("ping", at, streamPing(payload.Ping))
	case *investapi.MarketDataResponse_SubscribeCandlesResponse:
		return subscriptionEvent("candles", payload.SubscribeCandlesResponse, at)
	case *investapi.MarketDataResponse_SubscribeOrderBookResponse:
		return subscriptionEvent("orderbook", payload.SubscribeOrderBookResponse, at)
	case *investapi.MarketDataResponse_SubscribeTradesResponse:
		return subscriptionEvent("trades", payload.SubscribeTradesResponse, at)
	case *investapi.MarketDataResponse_SubscribeInfoResponse:
		return subscriptionEvent("info", payload.SubscribeInfoResponse, at)
	case *investapi.MarketDataResponse_SubscribeLastPriceResponse:
		return subscriptionEvent("last_price", payload.SubscribeLastPriceResponse, at)
	default:
		return unknownStreamEvent(response, at)
	}
}

type SubscriptionView struct {
	Kind   string          `json:"kind"`
	Result json.RawMessage `json:"result"`
}

func subscriptionEvent(kind string, result proto.Message, at time.Time) StreamEvent {
	return NewStreamEvent("subscription", at, SubscriptionView{Kind: kind, Result: protoView(result)})
}

func PortfolioStreamEvent(response *investapi.PortfolioStreamResponse, at time.Time) StreamEvent {
	if value := response.GetPortfolio(); value != nil {
		event := NewStreamEvent("portfolio", at, Portfolio(value))
		event.AccountID = value.GetAccountId()
		return event
	}
	if value := response.GetPing(); value != nil {
		return NewStreamEvent("ping", at, streamPing(value))
	}
	if value := response.GetSubscriptions(); value != nil {
		return subscriptionEvent("portfolio", value, at)
	}
	return unknownStreamEvent(response, at)
}

func PositionsStreamEvent(response *investapi.PositionsStreamResponse, at time.Time) StreamEvent {
	if value := response.GetInitialPositions(); value != nil {
		event := NewStreamEvent("positions_snapshot", at, Positions(value))
		event.AccountID = value.GetAccountId()
		return event
	}
	if value := response.GetPosition(); value != nil {
		event := NewStreamEvent("positions", at, positionDelta(value))
		event.AccountID = value.GetAccountId()
		return event
	}
	if value := response.GetPing(); value != nil {
		return NewStreamEvent("ping", at, streamPing(value))
	}
	if value := response.GetSubscriptions(); value != nil {
		return subscriptionEvent("positions", value, at)
	}
	return unknownStreamEvent(response, at)
}

type PositionMoneyView struct {
	Available Decimal `json:"available"`
	Blocked   Decimal `json:"blocked"`
}

type PositionDeltaView struct {
	AccountID  string                   `json:"account_id"`
	ChangeTime string                   `json:"change_time,omitempty"`
	Money      []PositionMoneyView      `json:"money"`
	Securities []SecurityPositionView   `json:"securities"`
	Futures    []DerivativePositionView `json:"futures"`
	Options    []DerivativePositionView `json:"options"`
}

func positionDelta(value *investapi.PositionData) PositionDeltaView {
	view := PositionDeltaView{
		AccountID: value.GetAccountId(), ChangeTime: Timestamp(value.GetDate()),
		Money:      make([]PositionMoneyView, 0, len(value.GetMoney())),
		Securities: make([]SecurityPositionView, 0, len(value.GetSecurities())),
		Futures:    make([]DerivativePositionView, 0, len(value.GetFutures())),
		Options:    make([]DerivativePositionView, 0, len(value.GetOptions())),
	}
	for _, money := range value.GetMoney() {
		view.Money = append(view.Money, PositionMoneyView{Available: Money(money.GetAvailableValue()), Blocked: Money(money.GetBlockedValue())})
	}
	for _, position := range value.GetSecurities() {
		view.Securities = append(view.Securities, SecurityPositionView{
			InstrumentUID: position.GetInstrumentUid(), PositionUID: position.GetPositionUid(), FIGI: position.GetFigi(),
			Ticker: position.GetTicker(), ClassCode: position.GetClassCode(), InstrumentType: position.GetInstrumentType(),
			Balance: strconv.FormatInt(position.GetBalance(), 10), Blocked: strconv.FormatInt(position.GetBlocked(), 10),
			ExchangeBlocked: position.GetExchangeBlocked(),
		})
	}
	for _, position := range value.GetFutures() {
		view.Futures = append(view.Futures, DerivativePositionView{
			InstrumentUID: position.GetInstrumentUid(), PositionUID: position.GetPositionUid(), FIGI: position.GetFigi(),
			Ticker: position.GetTicker(), ClassCode: position.GetClassCode(), Balance: strconv.FormatInt(position.GetBalance(), 10),
			Blocked: strconv.FormatInt(position.GetBlocked(), 10),
		})
	}
	for _, position := range value.GetOptions() {
		view.Options = append(view.Options, DerivativePositionView{
			InstrumentUID: position.GetInstrumentUid(), PositionUID: position.GetPositionUid(),
			Ticker: position.GetTicker(), ClassCode: position.GetClassCode(), Balance: strconv.FormatInt(position.GetBalance(), 10),
			Blocked: strconv.FormatInt(position.GetBlocked(), 10),
		})
	}
	return view
}

type OrderTradeView struct {
	TradeID  string  `json:"trade_id"`
	Time     string  `json:"time,omitempty"`
	Price    Decimal `json:"price"`
	Quantity string  `json:"quantity"`
}

type OrderTradesView struct {
	OrderID       string           `json:"order_id"`
	AccountID     string           `json:"account_id"`
	InstrumentUID string           `json:"instrument_uid"`
	FIGI          string           `json:"figi,omitempty"`
	Direction     string           `json:"direction"`
	CreatedAt     string           `json:"created_at,omitempty"`
	Trades        []OrderTradeView `json:"trades"`
}

func OrdersStreamEvent(response *investapi.TradesStreamResponse, at time.Time) StreamEvent {
	if value := response.GetOrderTrades(); value != nil {
		trades := make([]OrderTradeView, 0, len(value.GetTrades()))
		for _, trade := range value.GetTrades() {
			trades = append(trades, OrderTradeView{
				TradeID: trade.GetTradeId(), Time: Timestamp(trade.GetDateTime()), Price: Quotation(trade.GetPrice()),
				Quantity: strconv.FormatInt(trade.GetQuantity(), 10),
			})
		}
		event := NewStreamEvent("order_trade", at, OrderTradesView{
			OrderID: value.GetOrderId(), AccountID: value.GetAccountId(), InstrumentUID: value.GetInstrumentUid(),
			FIGI: value.GetFigi(), Direction: value.GetDirection().String(), CreatedAt: Timestamp(value.GetCreatedAt()), Trades: trades,
		})
		event.AccountID = value.GetAccountId()
		return event
	}
	if value := response.GetPing(); value != nil {
		return NewStreamEvent("ping", at, streamPing(value))
	}
	if value := response.GetSubscription(); value != nil {
		return subscriptionEvent("orders", value, at)
	}
	return unknownStreamEvent(response, at)
}

func unknownStreamEvent(message proto.Message, at time.Time) StreamEvent {
	caseName := "nil_message"
	if message != nil {
		reflected := message.ProtoReflect()
		if reflected.IsValid() {
			caseName = "none"
			if payload := reflected.Descriptor().Oneofs().ByName("payload"); payload != nil {
				if field := reflected.WhichOneof(payload); field != nil {
					caseName = string(field.Name())
				}
			}
			if caseName == "none" {
				if fieldNumber, _, consumed := protowire.ConsumeTag(reflected.GetUnknown()); consumed > 0 {
					caseName = "unknown_field_" + strconv.Itoa(int(fieldNumber))
				}
			}
		}
	}
	return NewStreamEvent("unknown", at, UnknownStreamView{ProtobufOneofCase: caseName})
}

func protoView(value proto.Message) json.RawMessage {
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(value)
	if err != nil {
		return json.RawMessage(`{"marshal_error":true}`)
	}
	return data
}
