package render

import (
	"io"
	"strconv"

	investapi "github.com/Dronnn/tinvest/pb/investapi"
)

// LastPriceView is the JSON shape of one `quotes last` row. The broker's
// LastPrice already carries the resolved instrument_uid/ticker/class_code,
// so every row is self-describing regardless of which id shape the caller
// passed in.
type LastPriceView struct {
	InstrumentUID string  `json:"instrument_uid"`
	Ticker        string  `json:"ticker"`
	ClassCode     string  `json:"class_code"`
	FIGI          string  `json:"figi"`
	Price         Decimal `json:"price"`
	PriceType     string  `json:"price_type"`
	Time          string  `json:"time"`
}

// LastPrice converts a proto LastPrice to its view.
func LastPrice(p *investapi.LastPrice) LastPriceView {
	return LastPriceView{
		InstrumentUID: p.GetInstrumentUid(),
		Ticker:        p.GetTicker(),
		ClassCode:     p.GetClassCode(),
		FIGI:          p.GetFigi(),
		Price:         Quotation(p.GetPrice()),
		PriceType:     p.GetLastPriceType().String(),
		Time:          Timestamp(p.GetTime()),
	}
}

// LastPrices converts a proto LastPrice list, preserving order.
func LastPrices(list []*investapi.LastPrice) []LastPriceView {
	views := make([]LastPriceView, 0, len(list))
	for _, p := range list {
		views = append(views, LastPrice(p))
	}
	return views
}

// LastPricesTable renders `quotes last` results for humans.
func LastPricesTable(w io.Writer, views []LastPriceView) error {
	rows := make([][]string, 0, len(views))
	for _, v := range views {
		rows = append(rows, []string{v.InstrumentUID, v.Ticker, v.ClassCode, v.Price.Value, v.Time})
	}
	return Table(w, []string{"UID", "TICKER", "CLASS_CODE", "PRICE", "TIME"}, rows)
}

// ClosePriceView is the JSON shape of one `quotes close` row.
type ClosePriceView struct {
	InstrumentUID       string  `json:"instrument_uid"`
	Ticker              string  `json:"ticker"`
	ClassCode           string  `json:"class_code"`
	FIGI                string  `json:"figi"`
	Price               Decimal `json:"price"`
	EveningSessionPrice Decimal `json:"evening_session_price"`
	Time                string  `json:"time"`
}

// ClosePrice converts a proto InstrumentClosePriceResponse to its view.
func ClosePrice(p *investapi.InstrumentClosePriceResponse) ClosePriceView {
	return ClosePriceView{
		InstrumentUID:       p.GetInstrumentUid(),
		Ticker:              p.GetTicker(),
		ClassCode:           p.GetClassCode(),
		FIGI:                p.GetFigi(),
		Price:               Quotation(p.GetPrice()),
		EveningSessionPrice: Quotation(p.GetEveningSessionPrice()),
		Time:                Timestamp(p.GetTime()),
	}
}

// ClosePrices converts a proto close-price list, preserving order.
func ClosePrices(list []*investapi.InstrumentClosePriceResponse) []ClosePriceView {
	views := make([]ClosePriceView, 0, len(list))
	for _, p := range list {
		views = append(views, ClosePrice(p))
	}
	return views
}

// ClosePricesTable renders `quotes close` results for humans.
func ClosePricesTable(w io.Writer, views []ClosePriceView) error {
	rows := make([][]string, 0, len(views))
	for _, v := range views {
		rows = append(rows, []string{v.InstrumentUID, v.Ticker, v.ClassCode, v.Price.Value, v.Time})
	}
	return Table(w, []string{"UID", "TICKER", "CLASS_CODE", "PRICE", "TIME"}, rows)
}

// OrderBookLevelView is one price/quantity level of an order book side.
type OrderBookLevelView struct {
	Price    Decimal `json:"price"`
	Quantity string  `json:"quantity"`
}

// OrderBookLevel converts a proto Order (a book level, not a trading order)
// to its view.
func OrderBookLevel(o *investapi.Order) OrderBookLevelView {
	return OrderBookLevelView{
		Price:    Quotation(o.GetPrice()),
		Quantity: strconv.FormatInt(o.GetQuantity(), 10),
	}
}

// OrderBookLevels converts a proto level list, preserving order (best price
// first, as the broker returns it).
func OrderBookLevels(list []*investapi.Order) []OrderBookLevelView {
	views := make([]OrderBookLevelView, 0, len(list))
	for _, o := range list {
		views = append(views, OrderBookLevel(o))
	}
	return views
}

// OrderBookView is the JSON shape of `orderbook get`.
type OrderBookView struct {
	InstrumentUID string               `json:"instrument_uid"`
	Ticker        string               `json:"ticker"`
	ClassCode     string               `json:"class_code"`
	FIGI          string               `json:"figi"`
	Depth         int32                `json:"depth"`
	Bids          []OrderBookLevelView `json:"bids"`
	Asks          []OrderBookLevelView `json:"asks"`
	LastPrice     Decimal              `json:"last_price"`
	ClosePrice    Decimal              `json:"close_price"`
	LimitUp       Decimal              `json:"limit_up"`
	LimitDown     Decimal              `json:"limit_down"`
	OrderbookTime string               `json:"orderbook_time"`
}

// OrderBook converts a proto GetOrderBookResponse to its view.
func OrderBook(r *investapi.GetOrderBookResponse) OrderBookView {
	return OrderBookView{
		InstrumentUID: r.GetInstrumentUid(),
		Ticker:        r.GetTicker(),
		ClassCode:     r.GetClassCode(),
		FIGI:          r.GetFigi(),
		Depth:         r.GetDepth(),
		Bids:          OrderBookLevels(r.GetBids()),
		Asks:          OrderBookLevels(r.GetAsks()),
		LastPrice:     Quotation(r.GetLastPrice()),
		ClosePrice:    Quotation(r.GetClosePrice()),
		LimitUp:       Quotation(r.GetLimitUp()),
		LimitDown:     Quotation(r.GetLimitDown()),
		OrderbookTime: Timestamp(r.GetOrderbookTs()),
	}
}

// OrderBookTable renders an order book for humans: bids and asks as one
// table, best price first on each side.
func OrderBookTable(w io.Writer, v OrderBookView) error {
	rows := make([][]string, 0, len(v.Bids)+len(v.Asks))
	for _, b := range v.Bids {
		rows = append(rows, []string{"BID", b.Price.Value, b.Quantity})
	}
	for _, ask := range v.Asks {
		rows = append(rows, []string{"ASK", ask.Price.Value, ask.Quantity})
	}
	return Table(w, []string{"SIDE", "PRICE", "QTY"}, rows)
}
