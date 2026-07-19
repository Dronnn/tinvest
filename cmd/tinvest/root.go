package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	brokerinstruments "tinvest/internal/broker/instruments"
	brokerusers "tinvest/internal/broker/users"
	"tinvest/internal/config"
	"tinvest/internal/ratelimit"
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
	// connectOverride supplies an in-process broker connection in command-flow
	// tests. Production leaves it nil and always uses transport.Dial.
	connectOverride func(context.Context, config.Settings) (*grpc.ClientConn, *render.CLIError)
}

const tariffRefreshTimeout = time.Second

func execute() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return executeContext(ctx)
}

func executeContext(ctx context.Context) int {
	a := &app{}
	root := a.rootCmd()
	err := root.ExecuteContext(ctx)
	if err == nil {
		return render.ExitOK
	}
	var exit *exitError
	if errors.As(err, &exit) {
		return exit.code
	}
	// Anything else is a cobra-level usage error (unknown command or flag);
	// cobra already printed the human-readable message to stderr. Emit the
	// stream contract as one NDJSON error frame, or the normal machine
	// envelope unless the user explicitly asked for tables.
	uerr := render.UsageError(err.Error())
	if isStreamInvocation(os.Args[1:]) {
		writer := render.NewNDJSONWriter(os.Stdout)
		event := render.NewStreamEvent("error", time.Now(), nil)
		event.Error = uerr.Body()
		_ = writer.Write(event)
		return uerr.ExitCode()
	}
	if render.Mode(outputMode(os.Args[1:]), os.Stdout) == "json" {
		_ = render.WriteJSON(os.Stdout, render.Failure(uerr, render.NewMeta("", "", 0)))
	}
	return uerr.ExitCode()
}

// outputMode resolves the output-format setting for the cobra-level usage-error
// path, which runs before flag parsing. It honors an explicit -o/--output flag
// found in the raw args (finding F17) and falls back to the environment, so
// `tinvest bogus -o json` still emits a JSON error envelope rather than deciding
// solely from TINVEST_OUTPUT.
func outputMode(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-o" || a == "--output":
			if i+1 < len(args) {
				return args[i+1]
			}
		case strings.HasPrefix(a, "--output="):
			return strings.TrimPrefix(a, "--output=")
		case strings.HasPrefix(a, "-o="):
			return strings.TrimPrefix(a, "-o=")
		case strings.HasPrefix(a, "-o") && len(a) > len("-o"):
			return a[len("-o"):]
		}
	}
	return os.Getenv(config.EnvOutput)
}

func isStreamInvocation(args []string) bool {
	valueFlags := map[string]bool{
		"--profile": true, "--account": true, "--output": true, "-o": true,
		"--token-file": true, "--timeout": true,
	}
	for i := 0; i < len(args); i++ {
		argument := args[i]
		if valueFlags[argument] {
			i++
			continue
		}
		if argument == "--sandbox" || argument == "--no-rate-limit" ||
			strings.HasPrefix(argument, "--sandbox=") || strings.HasPrefix(argument, "--no-rate-limit=") {
			continue
		}
		if strings.HasPrefix(argument, "--profile=") || strings.HasPrefix(argument, "--account=") ||
			strings.HasPrefix(argument, "--output=") || strings.HasPrefix(argument, "--token-file=") ||
			strings.HasPrefix(argument, "--timeout=") || strings.HasPrefix(argument, "-o=") ||
			(strings.HasPrefix(argument, "-o") && len(argument) > len("-o")) {
			continue
		}
		return argument == "stream"
	}
	return false
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
	pf.BoolVar(&a.flags.NoRateLimit, "no-rate-limit", false, "disable client-side unary rate limiting")

	root.AddCommand(
		a.versionCmd(), a.tokenCmd(), a.accountsCmd(), a.userCmd(),
		a.portfolioCmd(), a.positionsCmd(), a.balanceCmd(), a.operationsCmd(), a.tradesCmd(),
		a.instrumentsCmd(), a.researchCmd(), a.quotesCmd(), a.orderbookCmd(), a.candlesCmd(), a.signalsCmd(),
		a.ordersCmd(), a.stopOrdersCmd(), a.sandboxCmd(), a.streamCmd(),
	)
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
	if a.connectOverride != nil {
		return a.connectOverride(ctx, settings)
	}
	// The default retry policy is enabled for every connection: reads retry
	// automatically, mutations only when the call site opts in via
	// retry.Idempotent (plan §9). Enabling it here is safe because eligibility
	// is decided per-call, not per-connection.
	policy := retry.DefaultRetryPolicy()
	var limiter *ratelimit.Limiter
	if !settings.NoRateLimit {
		limiter = ratelimit.New(ratelimit.DefaultLimits(), ratelimit.DefaultMaxWait)
	}
	conn, err := transport.Dial(ctx, transport.Config{
		Endpoint:    settings.Endpoint,
		Token:       settings.Token,
		Timeout:     settings.Timeout,
		CAFile:      settings.CAFile,
		RetryPolicy: &policy,
		RateLimiter: limiter,
	})
	if err != nil {
		return nil, render.UsageError(fmt.Sprintf("invalid endpoint %q: %v", settings.Endpoint, err))
	}
	if limiter != nil {
		refreshTimeout := tariffRefreshTimeout
		if settings.Timeout > 0 && settings.Timeout < refreshTimeout {
			refreshTimeout = settings.Timeout
		}
		refreshCtx, cancel := context.WithTimeout(ctx, refreshTimeout)
		_, _ = brokerusers.New(conn).Tariff(refreshCtx)
		cancel()
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
