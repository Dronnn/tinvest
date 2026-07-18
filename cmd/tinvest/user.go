package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"tinvest/internal/broker/users"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type userTariffData struct {
	Tariff render.TariffView `json:"tariff"`
}

type userMarginData struct {
	Margin render.MarginView `json:"margin"`
}

func (a *app) userCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "user", Short: "User tariff and account attributes"}
	cmd.AddCommand(a.userTariffCmd(), a.userMarginCmd())
	return cmd
}

func (a *app) userTariffCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tariff",
		Short: "Get unary and stream limits for the current tariff",
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
			response, err := users.New(conn).Tariff(ctx)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			view := render.Tariff(response)
			if mode == "table" {
				return render.TariffTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(userTariffData{Tariff: view}, meta))
		},
	}
}

func (a *app) userMarginCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "margin",
		Short: "Get margin attributes for an account",
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
			response, err := users.New(conn).Margin(ctx, settings.AccountID)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			view := render.Margin(response)
			if mode == "table" {
				return render.MarginTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(userMarginData{Margin: view}, meta))
		},
	}
}
