package render

import (
	"io"
	"strconv"

	investapi "tinvest/internal/pb/investapi"
)

// SignalStrategyView is one analyst/technical signal strategy.
type SignalStrategyView struct {
	ID                     string  `json:"id"`
	Name                   string  `json:"name"`
	Description            string  `json:"description,omitempty"`
	URL                    string  `json:"url,omitempty"`
	Type                   string  `json:"type"`
	ActiveSignals          int32   `json:"active_signals"`
	TotalSignals           int32   `json:"total_signals"`
	TimeInPositionSeconds  string  `json:"time_in_position_seconds"`
	AverageSignalYield     Decimal `json:"average_signal_yield"`
	AverageSignalYieldYear Decimal `json:"average_signal_yield_year"`
	Yield                  Decimal `json:"yield"`
	YieldYear              Decimal `json:"yield_year"`
}

// SignalStrategies converts strategies, preserving broker order.
func SignalStrategies(strategies []*investapi.Strategy) []SignalStrategyView {
	views := make([]SignalStrategyView, 0, len(strategies))
	for _, strategy := range strategies {
		views = append(views, SignalStrategyView{
			ID: strategy.GetStrategyId(), Name: strategy.GetStrategyName(), Description: strategy.GetStrategyDescription(),
			URL: strategy.GetStrategyUrl(), Type: strategy.GetStrategyType().String(), ActiveSignals: strategy.GetActiveSignals(),
			TotalSignals: strategy.GetTotalSignals(), TimeInPositionSeconds: strconv.FormatInt(strategy.GetTimeInPosition(), 10),
			AverageSignalYield: Quotation(strategy.GetAverageSignalYield()), AverageSignalYieldYear: Quotation(strategy.GetAverageSignalYieldYear()),
			Yield: Quotation(strategy.GetYield()), YieldYear: Quotation(strategy.GetYieldYear()),
		})
	}
	return views
}

// SignalStrategiesTable renders signal strategies.
func SignalStrategiesTable(w io.Writer, views []SignalStrategyView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		rows = append(rows, []string{view.ID, view.Name, view.Type, strconv.Itoa(int(view.ActiveSignals)), strconv.Itoa(int(view.TotalSignals)), view.Yield.Value})
	}
	return Table(w, []string{"ID", "NAME", "TYPE", "ACTIVE", "TOTAL", "YIELD"}, rows)
}

// SignalView is one current or closed signal.
type SignalView struct {
	ID            string   `json:"id"`
	StrategyID    string   `json:"strategy_id"`
	StrategyName  string   `json:"strategy_name"`
	InstrumentUID string   `json:"instrument_uid"`
	CreatedAt     string   `json:"created_at,omitempty"`
	Direction     string   `json:"direction"`
	InitialPrice  Decimal  `json:"initial_price"`
	Info          string   `json:"info,omitempty"`
	Name          string   `json:"name"`
	TargetPrice   Decimal  `json:"target_price"`
	EndAt         string   `json:"end_at,omitempty"`
	Probability   *int32   `json:"probability,omitempty"`
	StopLoss      *Decimal `json:"stoploss,omitempty"`
	ClosePrice    *Decimal `json:"close_price,omitempty"`
	ClosedAt      string   `json:"closed_at,omitempty"`
}

// Signals converts signal records, preserving broker order.
func Signals(signals []*investapi.Signal) []SignalView {
	views := make([]SignalView, 0, len(signals))
	for _, signal := range signals {
		views = append(views, SignalView{
			ID: signal.GetSignalId(), StrategyID: signal.GetStrategyId(), StrategyName: signal.GetStrategyName(),
			InstrumentUID: signal.GetInstrumentUid(), CreatedAt: Timestamp(signal.GetCreateDt()), Direction: signal.GetDirection().String(),
			InitialPrice: Quotation(signal.GetInitialPrice()), Info: signal.GetInfo(), Name: signal.GetName(),
			TargetPrice: Quotation(signal.GetTargetPrice()), EndAt: Timestamp(signal.GetEndDt()), Probability: signal.Probability,
			StopLoss: optionalQuotation(signal.Stoploss), ClosePrice: optionalQuotation(signal.ClosePrice), ClosedAt: Timestamp(signal.GetCloseDt()),
		})
	}
	return views
}

func optionalQuotation(value *investapi.Quotation) *Decimal {
	if value == nil {
		return nil
	}
	converted := Quotation(value)
	return &converted
}

// SignalsTable renders signal records.
func SignalsTable(w io.Writer, views []SignalView) error {
	rows := make([][]string, 0, len(views))
	for _, view := range views {
		probability := ""
		if view.Probability != nil {
			probability = strconv.Itoa(int(*view.Probability))
		}
		rows = append(rows, []string{view.CreatedAt, view.ID, view.StrategyID, view.InstrumentUID, view.Direction, view.InitialPrice.Value, view.TargetPrice.Value, probability})
	}
	return Table(w, []string{"CREATED", "ID", "STRATEGY", "UID", "DIRECTION", "INITIAL", "TARGET", "PROBABILITY"}, rows)
}
