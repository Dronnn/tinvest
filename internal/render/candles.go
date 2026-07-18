package render

import (
	"io"
	"strconv"

	investapi "tinvest/internal/pb/investapi"
)

// CandleView is one historic OHLCV candle.
type CandleView struct {
	Time       string  `json:"time"`
	Open       Decimal `json:"open"`
	High       Decimal `json:"high"`
	Low        Decimal `json:"low"`
	Close      Decimal `json:"close"`
	Volume     string  `json:"volume"`
	VolumeBuy  string  `json:"volume_buy"`
	VolumeSell string  `json:"volume_sell"`
	IsComplete bool    `json:"is_complete"`
	Source     string  `json:"source"`
}

// Candles converts historic candles without changing completeness flags.
func Candles(candles []*investapi.HistoricCandle) []CandleView {
	views := make([]CandleView, 0, len(candles))
	for _, candle := range candles {
		views = append(views, CandleView{
			Time: Timestamp(candle.GetTime()), Open: Quotation(candle.GetOpen()), High: Quotation(candle.GetHigh()),
			Low: Quotation(candle.GetLow()), Close: Quotation(candle.GetClose()), Volume: strconv.FormatInt(candle.GetVolume(), 10),
			VolumeBuy: strconv.FormatInt(candle.GetVolumeBuy(), 10), VolumeSell: strconv.FormatInt(candle.GetVolumeSell(), 10),
			IsComplete: candle.GetIsComplete(), Source: candle.GetCandleSource().String(),
		})
	}
	return views
}

// CandlesTable renders historic candles.
func CandlesTable(w io.Writer, views []CandleView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{
			view.Time, view.Open.Value, view.High.Value, view.Low.Value, view.Close.Value,
			view.Volume, strconv.FormatBool(view.IsComplete),
		})
	}
	return Table(w, []string{"TIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "COMPLETE"}, rows)
}

// HistoryDownloadView reports one downloaded bulk history archive.
type HistoryDownloadView struct {
	InstrumentUID string `json:"instrument_uid"`
	Year          int    `json:"year"`
	Path          string `json:"path"`
	SizeBytes     string `json:"size_bytes"`
}

// HistoryDownloadTable renders a completed archive download.
func HistoryDownloadTable(w io.Writer, view HistoryDownloadView) error {
	return Table(w, []string{"FIELD", "VALUE"}, [][]string{
		{"instrument_uid", view.InstrumentUID}, {"year", strconv.Itoa(view.Year)},
		{"path", view.Path}, {"size_bytes", view.SizeBytes},
	})
}
