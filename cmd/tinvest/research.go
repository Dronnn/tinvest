package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	brokerresearch "github.com/Dronnn/tinvest/internal/broker/research"
	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
)

type researchNewsData struct {
	News       []render.NewsView `json:"news"`
	HasNext    bool              `json:"has_next"`
	NextCursor *string           `json:"next_cursor,omitempty"`
}

type researchFundamentalsData struct {
	Fundamentals []render.FundamentalView `json:"fundamentals"`
}

type researchForecastData struct {
	Targets   []render.ForecastTargetView   `json:"targets"`
	Consensus *render.ForecastConsensusView `json:"consensus,omitempty"`
}

type researchConsensusData struct {
	Forecasts []render.ConsensusForecastView `json:"forecasts"`
	Page      render.ResearchPageView        `json:"page"`
}

type researchInsiderDealsData struct {
	InsiderDeals []render.InsiderDealView `json:"insider_deals"`
	NextCursor   *string                  `json:"next_cursor,omitempty"`
}

func (a *app) researchCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "research", Short: "News, fundamentals, forecasts, and insider activity"}
	cmd.AddCommand(
		a.researchNewsCmd(), a.researchFundamentalsCmd(), a.researchForecastCmd(),
		a.researchConsensusCmd(), a.researchInsiderDealsCmd(),
	)
	return cmd
}

func (a *app) researchNewsCmd() *cobra.Command {
	var cursor int64
	var limit int32
	cmd := &cobra.Command{
		Use:   "news",
		Short: "Get one page of current news",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
			if limit <= 0 {
				return a.fail(mode, render.UsageError(fmt.Sprintf("invalid news limit %d: want a positive value", limit)), metaNoNet())
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			defer func() { _ = conn.Close() }()

			params := brokerresearch.NewsParams{Limit: &limit}
			if cmd.Flags().Changed("cursor") {
				params.Cursor = &cursor
			}
			ctx, info := transport.WithCallInfo(cmd.Context())
			result, err := brokerresearch.New(conn).News(ctx, params)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.News(result.Items)
			nextCursor := optionalInt64String(result.NextCursor)
			if mode == "table" {
				if err := render.NewsTable(os.Stdout, views); err != nil {
					return err
				}
				return render.PaginationTable(os.Stdout, stringValue(nextCursor))
			}
			return render.WriteJSON(os.Stdout, render.Success(researchNewsData{
				News: views, HasNext: result.HasNext, NextCursor: nextCursor,
			}, meta))
		},
	}
	cmd.Flags().Int64Var(&cursor, "cursor", 0, "cursor from a previous response")
	cmd.Flags().Int32Var(&limit, "limit", 1000, "news items per page")
	return cmd
}

func (a *app) researchFundamentalsCmd() *cobra.Command {
	var assets, instruments []string
	var noCache bool
	cmd := &cobra.Command{
		Use:   "fundamentals",
		Short: "Get fundamentals for asset UIDs or resolved instruments",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
			count := len(assets) + len(instruments)
			if count == 0 || count > 100 {
				return a.fail(mode, render.UsageError(fmt.Sprintf("invalid fundamentals asset count %d: want 1 through 100", count)), metaNoNet())
			}
			for _, asset := range assets {
				if strings.TrimSpace(asset) == "" {
					return a.fail(mode, render.UsageError("--asset must not be empty"), metaNoNet())
				}
			}
			if cerr := validateInstrumentIDs(instruments...); cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			defer func() { _ = conn.Close() }()

			assetUIDs := append([]string(nil), assets...)
			if len(instruments) > 0 {
				resolved, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, instruments, noCache)
				if resolveErr != nil {
					return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
				}
				for i, instrument := range resolved {
					if instrument.GetAssetUid() == "" {
						message := fmt.Sprintf("instrument %q has no asset_uid for fundamentals", instruments[i])
						return a.fail(mode, render.UsageError(message), metaNoNet())
					}
					assetUIDs = append(assetUIDs, instrument.GetAssetUid())
				}
			}
			ctx, info := transport.WithCallInfo(cmd.Context())
			values, err := brokerresearch.New(conn).Fundamentals(ctx, assetUIDs)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.Fundamentals(values)
			if mode == "table" {
				return render.FundamentalsTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(researchFundamentalsData{Fundamentals: views}, meta))
		},
	}
	cmd.Flags().StringArrayVar(&assets, "asset", nil, "asset UID (repeatable)")
	cmd.Flags().StringArrayVar(&instruments, "instrument", nil, "instrument id resolved to asset UID (repeatable: UID, FIGI, or TICKER@CLASSCODE)")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

func (a *app) researchForecastCmd() *cobra.Command {
	var instrument string
	var noCache bool
	cmd := &cobra.Command{
		Use:   "forecast",
		Short: "Get investment-house forecasts for one instrument",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
			if instrument == "" {
				return a.fail(mode, render.UsageError("--instrument is required"), metaNoNet())
			}
			if cerr := validateInstrumentIDs(instrument); cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			defer func() { _ = conn.Close() }()
			resolved, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, []string{instrument}, noCache)
			if resolveErr != nil {
				return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}
			ctx, info := transport.WithCallInfo(cmd.Context())
			result, err := brokerresearch.New(conn).Forecast(ctx, resolved[0].GetUid())
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			targets := render.ForecastTargets(result.Targets)
			consensus := render.ForecastConsensus(result.Consensus)
			if mode == "table" {
				if err := render.ForecastTargetsTable(os.Stdout, targets); err != nil {
					return err
				}
				if _, err := fmt.Fprintln(os.Stdout); err != nil {
					return err
				}
				return render.ForecastConsensusTable(os.Stdout, consensus)
			}
			return render.WriteJSON(os.Stdout, render.Success(researchForecastData{Targets: targets, Consensus: consensus}, meta))
		},
	}
	cmd.Flags().StringVar(&instrument, "instrument", "", "instrument id: UID, FIGI, or TICKER@CLASSCODE (required)")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

func (a *app) researchConsensusCmd() *cobra.Command {
	var limit, pageNumber int32
	cmd := &cobra.Command{
		Use:   "consensus",
		Short: "Get one page of instrument consensus forecasts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
			if limit <= 0 {
				return a.fail(mode, render.UsageError(fmt.Sprintf("invalid consensus limit %d: want a positive value", limit)), metaNoNet())
			}
			if pageNumber < 0 {
				return a.fail(mode, render.UsageError(fmt.Sprintf("invalid consensus page number %d: want zero or greater", pageNumber)), metaNoNet())
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			result, err := brokerresearch.New(conn).Consensus(ctx, brokerresearch.ConsensusParams{Limit: limit, PageNumber: pageNumber})
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.ConsensusForecasts(result.Items)
			page := render.ResearchPage(result.Page)
			if mode == "table" {
				if err := render.ConsensusForecastsTable(os.Stdout, views); err != nil {
					return err
				}
				if _, err := fmt.Fprintln(os.Stdout); err != nil {
					return err
				}
				return render.ResearchPageTable(os.Stdout, page)
			}
			return render.WriteJSON(os.Stdout, render.Success(researchConsensusData{Forecasts: views, Page: page}, meta))
		},
	}
	cmd.Flags().Int32Var(&pageNumber, "page-number", 0, "zero-based page number")
	cmd.Flags().Int32Var(&limit, "limit", 100, "consensus forecasts per page")
	return cmd
}

func (a *app) researchInsiderDealsCmd() *cobra.Command {
	var instrument, cursor string
	var limit int32
	var noCache bool
	cmd := &cobra.Command{
		Use:   "insider-deals",
		Short: "Get one page of insider deals for one instrument",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }
			if instrument == "" {
				return a.fail(mode, render.UsageError("--instrument is required"), metaNoNet())
			}
			if limit <= 0 || limit > 100 {
				return a.fail(mode, render.UsageError(fmt.Sprintf("invalid insider-deals limit %d: want 1 through 100", limit)), metaNoNet())
			}
			if cerr := validateInstrumentIDs(instrument); cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, metaNoNet())
			}
			defer func() { _ = conn.Close() }()
			resolved, resolveErr, trackingID := a.resolveAll(cmd.Context(), conn, []string{instrument}, noCache)
			if resolveErr != nil {
				return a.fail(mode, resolveErr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}
			ctx, info := transport.WithCallInfo(cmd.Context())
			result, err := brokerresearch.New(conn).InsiderDeals(ctx, brokerresearch.InsiderDealsParams{
				InstrumentID: resolved[0].GetUid(), Limit: limit, Cursor: cursor,
			})
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := render.InsiderDeals(result.Deals)
			if mode == "table" {
				if err := render.InsiderDealsTable(os.Stdout, views); err != nil {
					return err
				}
				return render.PaginationTable(os.Stdout, stringValue(result.NextCursor))
			}
			return render.WriteJSON(os.Stdout, render.Success(researchInsiderDealsData{
				InsiderDeals: views, NextCursor: result.NextCursor,
			}, meta))
		},
	}
	cmd.Flags().StringVar(&instrument, "instrument", "", "instrument id: UID, FIGI, or TICKER@CLASSCODE (required)")
	cmd.Flags().StringVar(&cursor, "cursor", "", "cursor from a previous response")
	cmd.Flags().Int32Var(&limit, "limit", 100, "insider deals per page (1 through 100)")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

func optionalInt64String(value *int64) *string {
	if value == nil {
		return nil
	}
	converted := strconv.FormatInt(*value, 10)
	return &converted
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
