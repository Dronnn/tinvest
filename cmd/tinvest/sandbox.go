package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"tinvest/internal/broker/sandbox"
	"tinvest/internal/config"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/policy"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

func (a *app) sandboxCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sandbox",
		Short: "Manage sandbox accounts (always targets the sandbox endpoint)",
	}
	cmd.AddCommand(
		a.sandboxOpenCmd(),
		a.sandboxCloseCmd(),
		a.sandboxAccountsCmd(),
		a.sandboxTopUpCmd(),
	)
	return cmd
}

// sandboxSettings loads settings like a.settings() but forces the sandbox
// endpoint regardless of profile or --sandbox flag state, warning on stderr
// when an override happened: a sandbox account mutation must never reach
// production, no matter what the active profile targets (plan §1.1).
func (a *app) sandboxSettings() (config.Settings, *render.CLIError) {
	settings, cerr := a.settings()
	if cerr != nil {
		return settings, cerr
	}
	if settings.Endpoint != config.SandboxEndpoint {
		fmt.Fprintf(os.Stderr, "warning: forcing sandbox endpoint for sandbox command (profile targeted %s)\n", settings.Endpoint)
		settings.Endpoint = config.SandboxEndpoint
	}
	return settings, nil
}

// checkSandboxKillSwitch enforces the kill switch for sandbox mutations
// (open/close/topup). These are not order intents, so they get no ledger
// entry, but a mutation is a mutation: the kill switch still blocks them
// (plan §1.1/§6).
func (a *app) checkSandboxKillSwitch(settings config.Settings) *render.CLIError {
	pol, err := policy.Load(settings.PolicyFile)
	if err != nil {
		return render.UsageError(err.Error())
	}
	if v := pol.CheckKillSwitch(); v != nil {
		return render.PolicyError(v.Message, v.Details)
	}
	return nil
}

func (a *app) sandboxOpenCmd() *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "open",
		Short: "Open a new sandbox account",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.sandboxSettings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := a.checkSandboxKillSwitch(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			resp, err := sandbox.New(conn).Open(ctx, name)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, true)), meta)
			}
			data := render.SandboxAccountView{AccountID: resp.GetAccountId()}
			if mode == "table" {
				return render.Table(os.Stdout, []string{"ACCOUNT_ID"}, [][]string{{data.AccountID}})
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "optional account name")
	return cmd
}

func (a *app) sandboxCloseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "close <account-id>",
		Short: "Close a sandbox account",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.sandboxSettings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := a.checkSandboxKillSwitch(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			_, err := sandbox.New(conn).Close(ctx, args[0])
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, true)), meta)
			}
			data := render.SandboxAccountView{AccountID: args[0]}
			if mode == "table" {
				return render.Table(os.Stdout, []string{"ACCOUNT_ID"}, [][]string{{data.AccountID}})
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
	return cmd
}

func (a *app) sandboxAccountsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "accounts",
		Short: "List sandbox accounts visible to the token",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			// A read, not a mutation: no kill-switch check (plan §1.1).
			settings, cerr := a.sandboxSettings()
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
			list, err := sandbox.New(conn).Accounts(ctx)
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

func (a *app) sandboxTopUpCmd() *cobra.Command {
	var amount, currency string
	cmd := &cobra.Command{
		Use:   "topup",
		Short: "Credit virtual money to a sandbox account (SandboxPayIn)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.sandboxSettings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			q, err := render.ParseQuotation(amount)
			if err != nil {
				return a.fail(mode, render.UsageError(fmt.Sprintf("invalid --amount %q: %v", amount, err)), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if q.GetUnits() < 0 || q.GetNano() < 0 {
				return a.fail(mode, render.UsageError("--amount must not be negative"), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if q.GetUnits() == 0 && q.GetNano() == 0 {
				return a.fail(mode, render.UsageError("--amount must be positive"), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if cerr := a.checkSandboxKillSwitch(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			money := &investapi.MoneyValue{Units: q.GetUnits(), Nano: q.GetNano(), Currency: strings.ToLower(currency)}
			ctx, info := transport.WithCallInfo(cmd.Context())
			resp, err := sandbox.New(conn).PayIn(ctx, settings.AccountID, money)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, true)), meta)
			}
			data := render.SandboxBalance(settings.AccountID, resp)
			if mode == "table" {
				bal := ""
				if data.Balance != nil {
					bal = data.Balance.Value
				}
				return render.Table(os.Stdout, []string{"ACCOUNT_ID", "BALANCE"}, [][]string{{data.AccountID, bal}})
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
	cmd.Flags().StringVar(&amount, "amount", "", "amount to credit as a decimal string (required)")
	cmd.Flags().StringVar(&currency, "currency", "rub", "currency code (the API documents this as a ruble top-up)")
	return cmd
}
