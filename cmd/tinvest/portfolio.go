package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	brokerportfolio "tinvest/internal/broker/portfolio"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type portfolioGetData struct {
	Portfolio render.PortfolioView `json:"portfolio"`
}

type positionsGetData struct {
	Positions render.PositionsView `json:"positions"`
}

type balanceGetData struct {
	Balance render.BalanceView `json:"balance"`
}

func (a *app) portfolioCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "portfolio", Short: "Portfolio totals and holdings"}
	cmd.AddCommand(a.portfolioGetCmd())
	return cmd
}

func (a *app) portfolioGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Get portfolio totals, yield, and positions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			response, err := brokerportfolio.New(conn).Portfolio(ctx, settings.AccountID)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			view := render.Portfolio(response)
			if mode == "table" {
				return render.PortfolioTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(portfolioGetData{Portfolio: view}, meta))
		},
	}
}

func (a *app) positionsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "positions", Short: "Account positions and blocked quantities"}
	cmd.AddCommand(a.positionsGetCmd())
	return cmd
}

func (a *app) positionsGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Get money, securities, futures, and options positions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			response, err := brokerportfolio.New(conn).Positions(ctx, settings.AccountID)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			view := render.Positions(response)
			if mode == "table" {
				return render.PositionsTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(positionsGetData{Positions: view}, meta))
		},
	}
}

func (a *app) balanceCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "balance", Short: "Withdrawable and blocked money"}
	cmd.AddCommand(a.balanceGetCmd())
	return cmd
}

func (a *app) balanceGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Get withdraw limits summarized by currency",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			response, err := brokerportfolio.New(conn).WithdrawLimits(ctx, settings.AccountID)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			view := render.Balance(response)
			if mode == "table" {
				return render.BalanceTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(balanceGetData{Balance: view}, meta))
		},
	}
}
