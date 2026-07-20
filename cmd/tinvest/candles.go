package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/Dronnn/tinvest/internal/broker/history"
	"github.com/Dronnn/tinvest/internal/broker/marketdata"
	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
)

type candlesGetData struct {
	InstrumentUID string              `json:"instrument_uid"`
	Interval      string              `json:"interval"`
	From          string              `json:"from"`
	To            string              `json:"to"`
	Candles       []render.CandleView `json:"candles"`
}

type candlesDownloadData struct {
	Download render.HistoryDownloadView `json:"download"`
}

func (a *app) candlesCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "candles", Short: "Historic candles and bulk archives"}
	cmd.AddCommand(a.candlesGetCmd(), a.candlesDownloadCmd())
	return cmd
}

func (a *app) candlesGetCmd() *cobra.Command {
	var intervalRaw, fromRaw, toRaw string
	var noCache bool
	cmd := &cobra.Command{
		Use:   "get <id>",
		Short: "Get historic candles with automatic range windowing",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
			interval, err := marketdata.ParseCandleInterval(intervalRaw)
			if err != nil {
				return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
			}
			from, to, err := parseRequiredTimeRange(fromRaw, toRaw)
			if err != nil {
				return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
			}
			if cerr := validateInstrumentIDs(args[0]); cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			defer func() { _ = conn.Close() }()

			instruments, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, args, noCache)
			if resolveErr != nil {
				return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}
			uid := instruments[0].GetUid()
			ctx, info := transport.WithCallInfo(cmd.Context())
			values, err := marketdata.New(conn).Candles(ctx, uid, interval, from, to)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.Candles(values)
			if mode == "table" {
				return render.CandlesTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(candlesGetData{
				InstrumentUID: uid, Interval: intervalRaw, From: from.Format(time.RFC3339), To: to.Format(time.RFC3339), Candles: views,
			}, meta))
		},
	}
	cmd.Flags().StringVar(&intervalRaw, "interval", "", "1m, 2m, 3m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 1d, 1w, or 1M")
	cmd.Flags().StringVar(&fromRaw, "from", "", "period start as RFC3339 (required)")
	cmd.Flags().StringVar(&toRaw, "to", "", "period end as RFC3339 (required)")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

func (a *app) candlesDownloadCmd() *cobra.Command {
	var year int
	var outDir string
	var noCache bool
	cmd := &cobra.Command{
		Use:   "download <id>",
		Short: "Download a yearly bulk candle-history zip",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
			if err := history.ValidateYear(year); err != nil {
				return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
			}
			if cerr := validateInstrumentIDs(args[0]); cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			defer func() { _ = conn.Close() }()
			instruments, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, args, noCache)
			if resolveErr != nil {
				return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}
			uid := instruments[0].GetUid()

			client, err := history.New(settings.Token, settings.CAFile)
			if err != nil {
				return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
			}
			downloadContext, cancel := context.WithTimeout(cmd.Context(), effectiveCallTimeout(settings.Timeout))
			defer cancel()
			result, err := client.Download(downloadContext, uid, year, outDir)
			meta := render.NewMeta(settings.AccountID, "", time.Since(start))
			if err != nil {
				return a.fail(mode, classifyHistoryError(err), meta)
			}
			view := render.HistoryDownloadView{
				InstrumentUID: uid, Year: year, Path: result.Path, SizeBytes: strconv.FormatInt(result.Size, 10),
			}
			if mode == "table" {
				return render.HistoryDownloadTable(os.Stdout, view)
			}
			return render.WriteJSON(os.Stdout, render.Success(candlesDownloadData{Download: view}, meta))
		},
	}
	cmd.Flags().IntVar(&year, "year", 0, "four-digit archive year (required)")
	cmd.Flags().StringVar(&outDir, "out", ".", "output directory")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

func effectiveCallTimeout(configured time.Duration) time.Duration {
	if configured <= 0 {
		return transport.DefaultTimeout
	}
	return configured
}

func classifyHistoryError(err error) *render.CLIError {
	var httpErr *history.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return render.AuthError(err.Error())
		case http.StatusTooManyRequests:
			return &render.CLIError{Code: render.CodeRateLimited, Message: err.Error(), Retryable: true}
		default:
			if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
				return &render.CLIError{Code: render.CodeBrokerRejected, Message: err.Error()}
			}
			return &render.CLIError{Code: render.CodeNetwork, Message: err.Error(), Retryable: true}
		}
	}
	if errors.Is(err, os.ErrExist) {
		return render.UsageError(err.Error())
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return render.UsageError(err.Error())
	}
	return &render.CLIError{Code: render.CodeNetwork, Message: err.Error(), Retryable: true}
}
