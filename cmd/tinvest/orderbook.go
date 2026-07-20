package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/Dronnn/tinvest/internal/broker/marketdata"
	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
)

type orderbookGetData struct {
	OrderBook render.OrderBookView `json:"orderbook"`
}

func (a *app) orderbookCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "orderbook",
		Short: "Order book (market depth)",
	}
	cmd.AddCommand(a.orderbookGetCmd())
	return cmd
}

func (a *app) orderbookGetCmd() *cobra.Command {
	var depth int32
	var noCache bool
	cmd := &cobra.Command{
		Use:   "get <id>",
		Short: "Order book for an instrument",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if err := marketdata.ValidateDepth(depth); err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			// Validate identifier syntax before requiring a token/connection so
			// garbage input fails with exit 2 rather than an auth error (plan §7).
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
			book, err := marketdata.New(conn).OrderBook(ctx, insts[0].GetUid(), depth)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}

			view := render.OrderBook(book)
			if mode == "table" {
				return render.OrderBookTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(orderbookGetData{OrderBook: view}, meta))
		},
	}
	cmd.Flags().Int32Var(&depth, "depth", 20, "order book depth: 1, 10, 20, 30, 40, or 50")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}
