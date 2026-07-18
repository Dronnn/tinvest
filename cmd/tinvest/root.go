package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	brokerinstruments "tinvest/internal/broker/instruments"
	"tinvest/internal/config"
	"tinvest/internal/render"
	"tinvest/internal/transport"
	"tinvest/internal/transport/retry"
)

// exitError carries a process exit code out of a command whose envelope has
// already been written.
type exitError struct{ code int }

func (e *exitError) Error() string { return fmt.Sprintf("exit code %d", e.code) }

type app struct {
	flags config.Flags
	// ledgerDir overrides the intent-journal directory; empty means
	// ledger.DefaultDir. Set by tests to an isolated temp dir.
	ledgerDir string
}

func execute() int {
	a := &app{}
	root := a.rootCmd()
	err := root.Execute()
	if err == nil {
		return render.ExitOK
	}
	var exit *exitError
	if errors.As(err, &exit) {
		return exit.code
	}
	// Anything else is a cobra-level usage error (unknown command or flag);
	// cobra already printed the human-readable message to stderr. Emit the
	// machine envelope unless the user explicitly asked for tables.
	uerr := render.UsageError(err.Error())
	if render.Mode(os.Getenv(config.EnvOutput), os.Stdout) == "json" {
		_ = render.WriteJSON(os.Stdout, render.Failure(uerr, render.NewMeta("", "", 0)))
	}
	return uerr.ExitCode()
}

func (a *app) rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "tinvest",
		Short:         "T-Invest broker adapter CLI",
		Long:          "tinvest is a stateless command-line adapter for the T-Bank Invest gRPC API:\nvalidate, transmit, report. Machine-first JSON output with a stable envelope\nand exit-code contract.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	pf := root.PersistentFlags()
	pf.StringVar(&a.flags.Profile, "profile", "", "config profile name (env TINVEST_PROFILE)")
	pf.StringVar(&a.flags.AccountID, "account", "", "account id for account-scoped commands")
	pf.StringVarP(&a.flags.Output, "output", "o", "", "output format: json or table (env TINVEST_OUTPUT)")
	pf.StringVar(&a.flags.TokenFile, "token-file", "", "file containing the API token (overrides TINVEST_TOKEN)")
	pf.DurationVar(&a.flags.Timeout, "timeout", 0, "per-call deadline (default 10s)")
	pf.BoolVar(&a.flags.Sandbox, "sandbox", false, "shortcut: use the sandbox endpoint")

	root.AddCommand(a.versionCmd(), a.tokenCmd(), a.accountsCmd(), a.instrumentsCmd(), a.quotesCmd(), a.orderbookCmd(), a.ordersCmd())
	return root
}

// settings resolves configuration, mapping failures to the proper error class.
func (a *app) settings() (config.Settings, *render.CLIError) {
	settings, err := config.Load(a.flags)
	if err == nil {
		return settings, nil
	}
	var tokenErr *config.TokenError
	if errors.As(err, &tokenErr) {
		return config.Settings{}, render.AuthError(err.Error())
	}
	return config.Settings{}, render.UsageError(err.Error())
}

// connect dials the broker; it fails fast with an auth error when no token
// source is configured.
func (a *app) connect(ctx context.Context, settings config.Settings) (*grpc.ClientConn, *render.CLIError) {
	if settings.Token == "" {
		return nil, render.AuthError("no token configured: set TINVEST_TOKEN, use --token-file, or configure token_file in a profile")
	}
	// The default retry policy is enabled for every connection: reads retry
	// automatically, mutations only when the call site opts in via
	// retry.Idempotent (plan §9). Enabling it here is safe because eligibility
	// is decided per-call, not per-connection.
	policy := retry.DefaultRetryPolicy()
	conn, err := transport.Dial(ctx, transport.Config{
		Endpoint:    settings.Endpoint,
		Token:       settings.Token,
		Timeout:     settings.Timeout,
		RetryPolicy: &policy,
	})
	if err != nil {
		return nil, render.UsageError(fmt.Sprintf("invalid endpoint %q: %v", settings.Endpoint, err))
	}
	return conn, nil
}

// validateInstrumentIDs checks the syntax of every instrument identifier
// locally, before any token or connection is required (plan §7 precedence:
// garbage input yields exit 2 even without a token). It is the syntactic half
// of resolveAll's classification, pulled ahead of connect.
func validateInstrumentIDs(ids ...string) *render.CLIError {
	for _, id := range ids {
		if _, err := brokerinstruments.Classify(id); err != nil {
			return render.UsageError(err.Error())
		}
	}
	return nil
}

// requireAccount enforces that a mutating command has an account (plan §8:
// never guess). Returns a usage error when none is configured or passed.
func requireAccount(settings config.Settings) *render.CLIError {
	if settings.AccountID == "" {
		return render.UsageError("account required for this command: pass --account or configure account_id in a profile")
	}
	return nil
}

// fail reports a classified error: JSON envelope on stdout in JSON mode, a
// plain diagnostic on stderr otherwise. It returns the exit-code carrier.
func (a *app) fail(mode string, cerr *render.CLIError, meta render.Meta) error {
	if mode == "json" {
		_ = render.WriteJSON(os.Stdout, render.Failure(cerr, meta))
	} else {
		fmt.Fprintf(os.Stderr, "error: %s\n", cerr.Message)
	}
	return &exitError{cerr.ExitCode()}
}

// callContext converts transport observations for error classification.
func callContext(info *transport.CallInfo, mutation bool) render.CallContext {
	return render.CallContext{
		Phase:      info.Phase(),
		TrackingID: info.TrackingID(),
		RetryAfter: info.RetryAfter(),
		APIMessage: info.APIMessage(),
		Mutation:   mutation,
	}
}
