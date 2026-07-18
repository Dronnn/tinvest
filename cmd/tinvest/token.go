package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"tinvest/internal/broker/accounts"
	"tinvest/internal/broker/users"
	"tinvest/internal/config"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type tokenCheckData struct {
	UserID               string               `json:"user_id,omitempty"`
	Tariff               string               `json:"tariff,omitempty"`
	PremStatus           bool                 `json:"prem_status"`
	QualStatus           bool                 `json:"qual_status"`
	QualifiedForWorkWith []string             `json:"qualified_for_work_with,omitempty"`
	TokenHints           []string             `json:"token_hints,omitempty"`
	Accounts             []render.AccountView `json:"accounts"`
}

func (a *app) tokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "API token utilities",
	}
	cmd.AddCommand(a.tokenCheckCmd())
	return cmd
}

func (a *app) tokenCheckCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Validate the token and report its access",
		Long:  "Calls GetInfo and GetAccounts with the resolved token and reports the user\nprofile, the visible accounts with their access levels, and hints about what\nkind of token this looks like. Exits 3 when the broker rejects the token.",
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

			infoCtx, infoCall := transport.WithCallInfo(cmd.Context())
			userInfo, err := users.New(conn).Info(infoCtx)
			if err != nil {
				meta := render.NewMeta(settings.AccountID, infoCall.TrackingID(), time.Since(start))
				return a.fail(mode, render.Classify(err, callContext(infoCall, false)), meta)
			}

			listCtx, listCall := transport.WithCallInfo(cmd.Context())
			list, err := accounts.New(conn).List(listCtx)
			meta := render.NewMeta(settings.AccountID, listCall.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(listCall, false)), meta)
			}

			data := tokenCheckData{
				UserID:               userInfo.GetUserId(),
				Tariff:               userInfo.GetTariff(),
				PremStatus:           userInfo.GetPremStatus(),
				QualStatus:           userInfo.GetQualStatus(),
				QualifiedForWorkWith: userInfo.GetQualifiedForWorkWith(),
				TokenHints:           tokenHints(settings, list),
				Accounts:             render.Accounts(list),
			}
			if mode == "table" {
				fmt.Printf("user_id  %s\ntariff   %s\nprem     %t\nqual     %t\n", data.UserID, data.Tariff, data.PremStatus, data.QualStatus)
				for _, h := range data.TokenHints {
					fmt.Printf("hint     %s\n", h)
				}
				fmt.Println()
				return render.AccountsTable(os.Stdout, data.Accounts)
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
}

// tokenHints derives what kind of token this looks like from observable
// facts; the API itself does not report the token kind.
func tokenHints(settings config.Settings, list []*investapi.Account) []string {
	var hints []string
	if settings.Endpoint == config.SandboxEndpoint {
		hints = append(hints, "sandbox endpoint: this is a sandbox token")
	}
	if len(list) == 0 {
		hints = append(hints, "token sees no accounts (account-scoped, or no open accounts)")
		return hints
	}
	readOnly, full := 0, 0
	for _, acc := range list {
		switch acc.GetAccessLevel() {
		case investapi.AccessLevel_ACCOUNT_ACCESS_LEVEL_READ_ONLY:
			readOnly++
		case investapi.AccessLevel_ACCOUNT_ACCESS_LEVEL_FULL_ACCESS:
			full++
		}
	}
	switch {
	case readOnly == len(list):
		hints = append(hints, "all accounts read-only: this looks like a read-only token")
	case full > 0:
		hints = append(hints, fmt.Sprintf("full access to %d of %d account(s)", full, len(list)))
	}
	return hints
}
