package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"tinvest/internal/broker/accounts"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type accountsListData struct {
	Accounts []render.AccountView `json:"accounts"`
}

func (a *app) accountsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "accounts",
		Short: "Brokerage accounts",
	}
	cmd.AddCommand(a.accountsListCmd())
	return cmd
}

func (a *app) accountsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List accounts visible to the token",
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
			list, err := accounts.New(conn).List(ctx)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}

			views := render.Accounts(list)
			if mode == "table" {
				return render.AccountsTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(accountsListData{Accounts: views}, meta))
		},
	}
}
