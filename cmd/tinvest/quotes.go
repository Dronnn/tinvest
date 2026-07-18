package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"tinvest/internal/broker/marketdata"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type quotesLastData struct {
	LastPrices []render.LastPriceView `json:"last_prices"`
}

type quotesCloseData struct {
	ClosePrices []render.ClosePriceView `json:"close_prices"`
}

func (a *app) quotesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "quotes",
		Short: "Market quotes",
	}
	cmd.AddCommand(a.quotesLastCmd(), a.quotesCloseCmd())
	return cmd
}

func (a *app) quotesLastCmd() *cobra.Command {
	var noCache bool
	cmd := &cobra.Command{
		Use:   "last <id...>",
		Short: "Last trade price for one or more instruments",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := validateInstrumentIDs(args...); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			insts, cerr, trackingID := a.resolveAll(cmd.Context(), conn, args, noCache)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}

			ctx, info := transport.WithCallInfo(cmd.Context())
			list, err := marketdata.New(conn).LastPrices(ctx, instrumentUIDs(insts))
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}

			views := render.LastPrices(list)
			if mode == "table" {
				return render.LastPricesTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(quotesLastData{LastPrices: views}, meta))
		},
	}
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

func (a *app) quotesCloseCmd() *cobra.Command {
	var noCache bool
	cmd := &cobra.Command{
		Use:   "close <id...>",
		Short: "Trading-session close price for one or more instruments",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := validateInstrumentIDs(args...); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			insts, cerr, trackingID := a.resolveAll(cmd.Context(), conn, args, noCache)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}

			ctx, info := transport.WithCallInfo(cmd.Context())
			list, err := marketdata.New(conn).ClosePrices(ctx, instrumentUIDs(insts))
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}

			views := render.ClosePrices(list)
			if mode == "table" {
				return render.ClosePricesTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(quotesCloseData{ClosePrices: views}, meta))
		},
	}
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}
