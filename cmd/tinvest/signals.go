package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	brokersignals "github.com/Dronnn/tinvest/internal/broker/signals"
	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
)

type signalStrategiesData struct {
	Strategies []render.SignalStrategyView `json:"strategies"`
}

type signalsListData struct {
	Signals []render.SignalView `json:"signals"`
}

func (a *app) signalsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "signals", Short: "Analyst and technical signals"}
	cmd.AddCommand(a.signalStrategiesCmd(), a.signalsListCmd())
	return cmd
}

func (a *app) signalStrategiesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "strategies",
		Short: "List signal strategies",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			strategies, err := brokersignals.New(conn).Strategies(ctx)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.SignalStrategies(strategies)
			if mode == "table" {
				return render.SignalStrategiesTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(signalStrategiesData{Strategies: views}, meta))
		},
	}
}

func (a *app) signalsListCmd() *cobra.Command {
	var strategyID string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List signals, optionally filtered by strategy",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			signals, err := brokersignals.New(conn).Signals(ctx, strategyID)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.Signals(signals)
			if mode == "table" {
				return render.SignalsTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(signalsListData{Signals: views}, meta))
		},
	}
	cmd.Flags().StringVar(&strategyID, "strategy", "", "strategy id filter")
	return cmd
}
