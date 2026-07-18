package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	brokerinstruments "tinvest/internal/broker/instruments"
	"tinvest/internal/broker/marketdata"
	"tinvest/internal/render"
	"tinvest/internal/transport"
)

type instrumentsListData struct {
	Instruments []render.ListedInstrumentView `json:"instruments"`
}

type dividendsData struct {
	Dividends []render.DividendView `json:"dividends"`
}

type couponsData struct {
	Coupons []render.CouponView `json:"coupons"`
}

type accruedInterestsData struct {
	AccruedInterests []render.AccruedInterestView `json:"accrued_interests"`
}

type schedulesData struct {
	Schedules []render.TradingScheduleView `json:"schedules"`
}

type tradingStatusData struct {
	TradingStatus render.TradingStatusView `json:"trading_status"`
}

func (a *app) instrumentsListCmd() *cobra.Command {
	var instrumentType string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List base instruments by type",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if err := brokerinstruments.ValidateType(instrumentType); err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			instruments, err := brokerinstruments.New(conn, nil).List(ctx, instrumentType)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.ListedInstruments(instruments)
			if mode == "table" {
				return render.ListedInstrumentsTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(instrumentsListData{Instruments: views}, meta))
		},
	}
	cmd.Flags().StringVar(&instrumentType, "type", "", "share, bond, etf, currency, future, or option")
	return cmd
}

type instrumentEventFlags struct {
	from    string
	to      string
	noCache bool
}

func (a *app) instrumentsDividendsCmd() *cobra.Command {
	var flags instrumentEventFlags
	cmd := &cobra.Command{
		Use:   "dividends <id>",
		Short: "List dividend events in a time range",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runInstrumentEvent(cmd, args[0], flags, "dividends")
		},
	}
	addInstrumentEventFlags(cmd, &flags)
	return cmd
}

func (a *app) instrumentsCouponsCmd() *cobra.Command {
	var flags instrumentEventFlags
	cmd := &cobra.Command{
		Use:   "coupons <id>",
		Short: "List bond coupon events in a time range",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runInstrumentEvent(cmd, args[0], flags, "coupons")
		},
	}
	addInstrumentEventFlags(cmd, &flags)
	return cmd
}

func (a *app) instrumentsAccruedInterestCmd() *cobra.Command {
	var flags instrumentEventFlags
	cmd := &cobra.Command{
		Use:   "accrued-interest <id>",
		Short: "List accrued bond interest in a time range",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runInstrumentEvent(cmd, args[0], flags, "accrued-interest")
		},
	}
	addInstrumentEventFlags(cmd, &flags)
	return cmd
}

func addInstrumentEventFlags(cmd *cobra.Command, flags *instrumentEventFlags) {
	cmd.Flags().StringVar(&flags.from, "from", "", "period start as RFC3339 (required)")
	cmd.Flags().StringVar(&flags.to, "to", "", "period end as RFC3339 (required)")
	cmd.Flags().BoolVar(&flags.noCache, "no-cache", false, "bypass the local instrument cache")
}

func (a *app) runInstrumentEvent(cmd *cobra.Command, rawID string, flags instrumentEventFlags, eventType string) error {
	start := time.Now()
	settings, cerr := a.settings()
	mode := render.Mode(settings.Output, os.Stdout)
	if cerr != nil {
		return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
	}
	metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
	from, to, err := parseRequiredTimeRange(flags.from, flags.to)
	if err != nil {
		return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
	}
	if cerr := validateInstrumentIDs(rawID); cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}
	conn, cerr := a.connect(cmd.Context(), settings)
	if cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}
	defer func() { _ = conn.Close() }()

	instruments, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, []string{rawID}, flags.noCache)
	if resolveErr != nil {
		return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
	}
	client := brokerinstruments.New(conn, nil)
	ctx, info := transport.WithCallInfo(cmd.Context())
	meta := func() render.Meta { return render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start)) }
	uid := instruments[0].GetUid()

	switch eventType {
	case "dividends":
		values, err := client.Dividends(ctx, uid, from, to)
		if err != nil {
			return a.fail(mode, render.Classify(err, callContext(info, false)), meta())
		}
		views := render.Dividends(values)
		if mode == "table" {
			return render.DividendsTable(os.Stdout, views)
		}
		return render.WriteJSON(os.Stdout, render.Success(dividendsData{Dividends: views}, meta()))
	case "coupons":
		values, err := client.Coupons(ctx, uid, from, to)
		if err != nil {
			return a.fail(mode, render.Classify(err, callContext(info, false)), meta())
		}
		views := render.Coupons(values)
		if mode == "table" {
			return render.CouponsTable(os.Stdout, views)
		}
		return render.WriteJSON(os.Stdout, render.Success(couponsData{Coupons: views}, meta()))
	case "accrued-interest":
		values, err := client.AccruedInterests(ctx, uid, from, to)
		if err != nil {
			return a.fail(mode, render.Classify(err, callContext(info, false)), meta())
		}
		views := render.AccruedInterests(values)
		if mode == "table" {
			return render.AccruedInterestsTable(os.Stdout, views)
		}
		return render.WriteJSON(os.Stdout, render.Success(accruedInterestsData{AccruedInterests: views}, meta()))
	default:
		return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: "unknown instrument event command"}, meta())
	}
}

func (a *app) instrumentsSchedulesCmd() *cobra.Command {
	var exchange, fromRaw, toRaw string
	cmd := &cobra.Command{
		Use:   "schedules",
		Short: "Get exchange trading schedules",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			from, to, err := parseRequiredTimeRange(fromRaw, toRaw)
			if err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			values, err := brokerinstruments.New(conn, nil).Schedules(ctx, exchange, from, to)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.TradingSchedules(values)
			if mode == "table" {
				return render.TradingSchedulesTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(schedulesData{Schedules: views}, meta))
		},
	}
	cmd.Flags().StringVar(&exchange, "exchange", "", "exchange name (default: all exchanges)")
	cmd.Flags().StringVar(&fromRaw, "from", "", "period start as RFC3339 (required)")
	cmd.Flags().StringVar(&toRaw, "to", "", "period end as RFC3339 (required)")
	return cmd
}

func (a *app) instrumentsTradingStatusCmd() *cobra.Command {
	var noCache bool
	cmd := &cobra.Command{
		Use:   "trading-status <id>",
		Short: "Get current trading and order availability",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := validateInstrumentIDs(args[0]); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()
			instruments, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, args, noCache)
			if resolveErr != nil {
				return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}

			ctx, info := transport.WithCallInfo(cmd.Context())
			status, err := marketdata.New(conn).TradingStatus(ctx, instruments[0].GetUid())
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			view := render.TradingStatus(status)
			if mode == "table" {
				return render.TradingStatusTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(tradingStatusData{TradingStatus: view}, meta))
		},
	}
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}
