package render

import (
	"io"
	"strconv"

	investapi "tinvest/internal/pb/investapi"
)

// InstrumentView is the JSON shape of a resolved instrument (plan §5/§8).
// Enums keep their proto names.
type InstrumentView struct {
	UID               string  `json:"uid"`
	FIGI              string  `json:"figi"`
	Ticker            string  `json:"ticker"`
	ClassCode         string  `json:"class_code"`
	Name              string  `json:"name"`
	Type              string  `json:"type"`
	Lot               int32   `json:"lot"`
	Currency          string  `json:"currency"`
	MinPriceIncrement Decimal `json:"min_price_increment"`
	TradingStatus     string  `json:"trading_status"`
}

// Instrument converts a proto instrument to its view.
func Instrument(i *investapi.Instrument) InstrumentView {
	return InstrumentView{
		UID:               i.GetUid(),
		FIGI:              i.GetFigi(),
		Ticker:            i.GetTicker(),
		ClassCode:         i.GetClassCode(),
		Name:              i.GetName(),
		Type:              i.GetInstrumentKind().String(),
		Lot:               i.GetLot(),
		Currency:          i.GetCurrency(),
		MinPriceIncrement: Quotation(i.GetMinPriceIncrement()),
		TradingStatus:     i.GetTradingStatus().String(),
	}
}

// InstrumentTable renders one instrument as a key/value table.
func InstrumentTable(w io.Writer, v InstrumentView) error {
	rows := [][]string{
		{"uid", v.UID},
		{"figi", v.FIGI},
		{"ticker", v.Ticker},
		{"class_code", v.ClassCode},
		{"name", v.Name},
		{"type", v.Type},
		{"lot", strconv.Itoa(int(v.Lot))},
		{"currency", v.Currency},
		{"min_price_increment", v.MinPriceIncrement.Value},
		{"trading_status", v.TradingStatus},
	}
	return Table(w, []string{"FIELD", "VALUE"}, rows)
}

// InstrumentShortView is the JSON shape of one FindInstrument search result.
type InstrumentShortView struct {
	UID       string `json:"uid"`
	FIGI      string `json:"figi"`
	Ticker    string `json:"ticker"`
	ClassCode string `json:"class_code"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Lot       int32  `json:"lot"`
}

// InstrumentShort converts a proto search hit to its view.
func InstrumentShort(i *investapi.InstrumentShort) InstrumentShortView {
	return InstrumentShortView{
		UID:       i.GetUid(),
		FIGI:      i.GetFigi(),
		Ticker:    i.GetTicker(),
		ClassCode: i.GetClassCode(),
		Name:      i.GetName(),
		Type:      i.GetInstrumentKind().String(),
		Lot:       i.GetLot(),
	}
}

// InstrumentsShort converts a proto search result list, preserving order.
func InstrumentsShort(list []*investapi.InstrumentShort) []InstrumentShortView {
	views := make([]InstrumentShortView, 0, len(list))
	for _, i := range list {
		views = append(views, InstrumentShort(i))
	}
	return views
}

// InstrumentsShortTable renders search results for humans.
func InstrumentsShortTable(w io.Writer, views []InstrumentShortView) error {
	rows := make([][]string, 0, len(views))
	for _, v := range views {
		rows = append(rows, []string{v.UID, v.Ticker, v.ClassCode, v.Type, v.Name})
	}
	return Table(w, []string{"UID", "TICKER", "CLASS_CODE", "TYPE", "NAME"}, rows)
}
