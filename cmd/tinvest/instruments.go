package main

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	brokerinstruments "tinvest/internal/broker/instruments"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type instrumentGetData struct {
	Instrument render.InstrumentView `json:"instrument"`
}

type instrumentsSearchData struct {
	Instruments []render.InstrumentShortView `json:"instruments"`
}

func (a *app) instrumentsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "instruments",
		Short: "Instrument reference data",
	}
	cmd.AddCommand(
		a.instrumentsGetCmd(), a.instrumentsSearchCmd(), a.instrumentsListCmd(),
		a.instrumentsDividendsCmd(), a.instrumentsCouponsCmd(), a.instrumentsAccruedInterestCmd(),
		a.instrumentsSchedulesCmd(), a.instrumentsTradingStatusCmd(),
	)
	return cmd
}

func (a *app) instrumentsGetCmd() *cobra.Command {
	var noCache bool
	cmd := &cobra.Command{
		Use:   "get <instrument_uid|figi|TICKER@CLASSCODE>",
		Short: "Resolve an instrument identifier to its full reference record",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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

			insts, cerr, trackingID := a.resolveAll(cmd.Context(), conn, args, noCache)
			meta := render.NewMeta(settings.AccountID, trackingID, time.Since(start))
			if cerr != nil {
				return a.fail(mode, cerr, meta)
			}

			view := render.Instrument(insts[0])
			if mode == "table" {
				return render.InstrumentTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(instrumentGetData{Instrument: view}, meta))
		},
	}
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

func (a *app) instrumentsSearchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "search <text>",
		Short: "Free-text instrument search",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
			list, err := brokerinstruments.New(conn, nil).Find(ctx, args[0])
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}

			views := render.InstrumentsShort(list)
			if mode == "table" {
				return render.InstrumentsShortTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(instrumentsSearchData{Instruments: views}, meta))
		},
	}
}

// instrumentCache builds the local resolver cache (plan §5): a JSON file at
// ${XDG_CACHE_HOME:-~/.cache}/tinvest/instruments.json, 24h TTL. A nil
// result (home directory unresolvable) disables caching for the run rather
// than failing the command.
func (a *app) instrumentCache() *brokerinstruments.Cache {
	path := brokerinstruments.DefaultCachePath()
	if path == "" {
		return nil
	}
	return brokerinstruments.NewCache(path, brokerinstruments.DefaultTTL, nil)
}

// resolveAll resolves every id in order, stopping at the first failure. It
// returns the tracking id of the failing call so callers can attach it to
// the failure meta (empty on success, and empty for a purely local usage
// error that never reached the broker).
func (a *app) resolveAll(cmdCtx context.Context, conn *grpc.ClientConn, ids []string, noCache bool) ([]*investapi.Instrument, *render.CLIError, string) {
	resolver := brokerinstruments.New(conn, a.instrumentCache())
	insts := make([]*investapi.Instrument, 0, len(ids))
	for _, id := range ids {
		ctx, info := transport.WithCallInfo(cmdCtx)
		inst, err := resolver.Resolve(ctx, id, noCache)
		if err != nil {
			return nil, classifyResolveErr(err, info), info.TrackingID()
		}
		insts = append(insts, inst)
	}
	return insts, nil, ""
}

// classifyResolveErr maps a resolution failure to the CLI error contract: a
// malformed identifier never reached the broker and is a usage error
// (exit 2); anything else came back from the wire and is classified as
// usual.
func classifyResolveErr(err error, info *transport.CallInfo) *render.CLIError {
	var invalid *brokerinstruments.InvalidIDError
	if errors.As(err, &invalid) {
		return render.UsageError(err.Error())
	}
	return render.Classify(err, callContext(info, false))
}

// instrumentUIDs extracts the resolved instrument_uid from each instrument,
// in order.
func instrumentUIDs(insts []*investapi.Instrument) []string {
	uids := make([]string, 0, len(insts))
	for _, inst := range insts {
		uids = append(uids, inst.GetUid())
	}
	return uids
}
