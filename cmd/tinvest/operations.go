package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	brokeroperations "tinvest/internal/broker/operations"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type operationsListData struct {
	Operations []render.OperationView `json:"operations"`
	NextCursor string                 `json:"next_cursor"`
}

type tradesListData struct {
	Trades     []render.ExecutedTradeView `json:"trades"`
	NextCursor string                     `json:"next_cursor"`
}

type operationListFlags struct {
	from       string
	to         string
	instrument string
	cursor     string
	limit      int32
	all        bool
}

func (a *app) operationsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "operations", Short: "Cursor-paginated account operations"}
	cmd.AddCommand(a.operationsListCmd())
	return cmd
}

func (a *app) operationsListCmd() *cobra.Command {
	var flags operationListFlags
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List account operations with cursor paging",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runOperationList(cmd, flags, false)
		},
	}
	addOperationListFlags(cmd, &flags)
	return cmd
}

func (a *app) tradesCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "trades", Short: "Executed trades from operation history"}
	cmd.AddCommand(a.tradesListCmd())
	return cmd
}

func (a *app) tradesListCmd() *cobra.Command {
	var flags operationListFlags
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List executions nested under executed operations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runOperationList(cmd, flags, true)
		},
	}
	addOperationListFlags(cmd, &flags)
	return cmd
}

func addOperationListFlags(cmd *cobra.Command, flags *operationListFlags) {
	cmd.Flags().StringVar(&flags.from, "from", "", "period start as RFC3339 (optional)")
	cmd.Flags().StringVar(&flags.to, "to", "", "period end as RFC3339 (optional)")
	cmd.Flags().StringVar(&flags.instrument, "instrument", "", "instrument id filter: uid, FIGI, or TICKER@CLASSCODE")
	cmd.Flags().StringVar(&flags.cursor, "cursor", "", "cursor from a previous response")
	cmd.Flags().Int32Var(&flags.limit, "limit", 100, "operations per page (3 through 1000)")
	cmd.Flags().BoolVar(&flags.all, "all", false, "follow cursors and return all pages")
}

func (a *app) runOperationList(cmd *cobra.Command, flags operationListFlags, trades bool) error {
	start := time.Now()
	settings, cerr := a.settings()
	mode := render.Mode(settings.Output, os.Stdout)
	if cerr != nil {
		return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
	}
	metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
	if cerr := requireAccount(settings); cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}
	if err := brokeroperations.ValidateLimit(flags.limit); err != nil {
		return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
	}
	from, to, err := parseOptionalTimeRange(flags.from, flags.to)
	if err != nil {
		return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
	}
	if flags.instrument != "" {
		if cerr := validateInstrumentIDs(flags.instrument); cerr != nil {
			return a.fail(mode, cerr, metaNoNet())
		}
	}

	conn, cerr := a.connect(cmd.Context(), settings)
	if cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}
	defer func() { _ = conn.Close() }()

	instrumentUID := ""
	if flags.instrument != "" {
		instruments, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, []string{flags.instrument}, false)
		if resolveErr != nil {
			return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
		}
		instrumentUID = instruments[0].GetUid()
	}

	state := investapi.OperationState_OPERATION_STATE_UNSPECIFIED
	if trades {
		state = investapi.OperationState_OPERATION_STATE_EXECUTED
	}
	ctx, info := transport.WithCallInfo(cmd.Context())
	result, err := brokeroperations.New(conn).List(ctx, brokeroperations.ListParams{
		AccountID: settings.AccountID, InstrumentID: instrumentUID, From: from, To: to,
		Cursor: flags.cursor, Limit: flags.limit, All: flags.all, State: state,
	})
	meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
	if err != nil {
		return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
	}
	if trades {
		views := render.ExecutedTrades(result.Items)
		if mode == "table" {
			if err := render.ExecutedTradesTable(os.Stdout, views); err != nil {
				return err
			}
			return render.PaginationTable(os.Stdout, result.NextCursor)
		}
		return render.WriteJSON(os.Stdout, render.Success(tradesListData{Trades: views, NextCursor: result.NextCursor}, meta))
	}
	views := render.Operations(result.Items)
	if mode == "table" {
		if err := render.OperationsTable(os.Stdout, views); err != nil {
			return err
		}
		return render.PaginationTable(os.Stdout, result.NextCursor)
	}
	return render.WriteJSON(os.Stdout, render.Success(operationsListData{Operations: views, NextCursor: result.NextCursor}, meta))
}
