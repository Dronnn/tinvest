package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	brokerstream "tinvest/internal/broker/streaming"
	"tinvest/internal/config"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	streamrunner "tinvest/internal/stream"
	"tinvest/internal/transport"
)

func (a *app) streamCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "stream", Short: "Resilient broker streams as NDJSON"}
	cmd.AddCommand(a.streamMarketDataCmd(), a.streamPortfolioCmd(), a.streamPositionsCmd(), a.streamOrdersCmd())
	return cmd
}

func (a *app) streamMarketDataCmd() *cobra.Command {
	var instruments []string
	var candles string
	var orderbook int32
	var trades, lastPrice, info bool
	cmd := &cobra.Command{
		Use:   "marketdata",
		Short: "Stream candles, order books, trades, last prices, and trading status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			writer := render.NewNDJSONWriter(os.Stdout)
			if cmd.Context().Err() != nil {
				return a.streamShutdown(writer, "")
			}
			settings, cerr := a.settings()
			if cerr != nil {
				return a.streamFail(writer, cerr, "")
			}
			instruments = brokerstream.UniqueInstrumentIDs(instruments)
			options := brokerstream.MarketDataOptions{
				CandleInterval: candles, OrderBookDepth: orderbook, Trades: trades, LastPrice: lastPrice, Info: info,
			}
			if _, err := brokerstream.MarketDataSubscriptions(instruments, options); err != nil {
				return a.streamFail(writer, render.UsageError(err.Error()), settings.AccountID)
			}
			if cerr := validateInstrumentIDs(instruments...); cerr != nil {
				return a.streamFail(writer, cerr, settings.AccountID)
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.streamFail(writer, cerr, settings.AccountID)
			}
			defer func() { _ = conn.Close() }()

			resolved, resolveErr, _ := a.resolveAll(cmd.Context(), conn, instruments, false)
			if resolveErr != nil {
				if cmd.Context().Err() != nil {
					return a.streamShutdown(writer, settings.AccountID)
				}
				return a.streamFail(writer, resolveErr, settings.AccountID)
			}
			uids := make([]string, 0, len(resolved))
			for _, instrument := range resolved {
				uids = append(uids, instrument.GetUid())
			}
			uids = brokerstream.UniqueInstrumentIDs(uids)
			registry, err := brokerstream.MarketDataSubscriptions(uids, options)
			if err != nil {
				return a.streamFail(writer, render.UsageError(err.Error()), settings.AccountID)
			}
			client := brokerstream.New(conn)
			runner := streamrunner.Runner[investapi.MarketDataRequest, investapi.MarketDataResponse]{
				Open: client.OpenMarketData, Subscriptions: registry,
				Watchdog:   streamrunner.DefaultWatchdog,
				IsActivity: brokerstream.MarketDataActivity,
				OnLifecycle: func(event streamrunner.LifecycleEvent) error {
					return writer.Write(render.LifecycleStreamEvent(event))
				},
				OnMessage: func(response *investapi.MarketDataResponse) error {
					return writer.Write(render.MarketDataStreamEvent(response, time.Now()))
				},
			}
			if orderbook != 0 {
				orderBookCutoffs := make(map[string]time.Time, len(uids))
				missingCutoffs := make(map[string]bool, len(uids))
				runner.Reconcile = func(ctx context.Context) error {
					cutoffs := make(map[string]time.Time, len(uids))
					missing := make(map[string]bool, len(uids))
					for _, uid := range uids {
						snapshot, err := client.OrderBookSnapshot(ctx, uid, orderbook)
						if err != nil {
							return err
						}
						if timestamp := snapshot.GetOrderbookTs(); timestamp != nil {
							cutoffs[uid] = timestamp.AsTime()
						} else {
							missing[uid] = true
						}
						if err := writer.Write(render.NewStreamEvent("snapshot", time.Now(), render.OrderBook(snapshot))); err != nil {
							return err
						}
					}
					orderBookCutoffs = cutoffs
					missingCutoffs = missing
					return nil
				}
				runner.BufferDuringReconcile = func(response *investapi.MarketDataResponse) bool {
					return response.GetOrderbook() != nil
				}
				runner.KeepAfterReconcile = func(response *investapi.MarketDataResponse) bool {
					book := response.GetOrderbook()
					if book == nil {
						return true
					}
					uid := book.GetInstrumentUid()
					if missingCutoffs[uid] || book.GetTime() == nil {
						return false
					}
					cutoff, found := orderBookCutoffs[uid]
					if !found {
						return true
					}
					return book.GetTime().AsTime().After(cutoff)
				}
				runner.KeepLiveAfterReconcile = func(response *investapi.MarketDataResponse) bool {
					book := response.GetOrderbook()
					if book == nil {
						return true
					}
					uid := book.GetInstrumentUid()
					if missingCutoffs[uid] {
						delete(missingCutoffs, uid)
						return false
					}
					cutoff, found := orderBookCutoffs[uid]
					if !found {
						return true
					}
					if book.GetTime() == nil {
						delete(orderBookCutoffs, uid)
						return false
					}
					return book.GetTime().AsTime().After(cutoff)
				}
			}
			return a.runStream(cmd.Context(), writer, settings.AccountID, runner.Run)
		},
	}
	cmd.Flags().StringArrayVar(&instruments, "instrument", nil, "instrument id (repeatable: UID, FIGI, or TICKER@CLASSCODE)")
	cmd.Flags().StringVar(&candles, "candles", "", "stream candles at interval 1m..1M (default 1m when omitted)")
	cmd.Flags().Lookup("candles").NoOptDefVal = "1m"
	cmd.Flags().Int32Var(&orderbook, "orderbook", 0, "stream order book at depth 1, 10, 20, 30, 40, or 50 (default 20 when omitted)")
	cmd.Flags().Lookup("orderbook").NoOptDefVal = "20"
	cmd.Flags().BoolVar(&trades, "trades", false, "stream public trades")
	cmd.Flags().BoolVar(&lastPrice, "last-price", false, "stream last prices")
	cmd.Flags().BoolVar(&info, "info", false, "stream trading status")
	return cmd
}

func (a *app) streamPortfolioCmd() *cobra.Command {
	return &cobra.Command{
		Use: "portfolio", Short: "Stream portfolio snapshots and updates", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			writer := render.NewNDJSONWriter(os.Stdout)
			if cmd.Context().Err() != nil {
				return a.streamShutdown(writer, "")
			}
			settings, conn, err := a.prepareAccountStream(cmd, writer)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			client := brokerstream.New(conn)
			runner := streamrunner.Runner[brokerstream.ServerRequest, investapi.PortfolioStreamResponse]{
				Open: func(ctx context.Context) (streamrunner.Session[brokerstream.ServerRequest, investapi.PortfolioStreamResponse], error) {
					return client.OpenPortfolio(ctx, settings.AccountID)
				},
				Watchdog:   streamrunner.DefaultWatchdog,
				IsActivity: brokerstream.PortfolioActivity,
				OnLifecycle: func(event streamrunner.LifecycleEvent) error {
					return writer.Write(withAccount(render.LifecycleStreamEvent(event), settings.AccountID))
				},
				OnMessage: func(response *investapi.PortfolioStreamResponse) error {
					return writer.Write(withAccount(render.PortfolioStreamEvent(response, time.Now()), settings.AccountID))
				},
			}
			return a.runStream(cmd.Context(), writer, settings.AccountID, runner.Run)
		},
	}
}

func (a *app) streamPositionsCmd() *cobra.Command {
	return &cobra.Command{
		Use: "positions", Short: "Stream initial positions and balance changes", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			writer := render.NewNDJSONWriter(os.Stdout)
			if cmd.Context().Err() != nil {
				return a.streamShutdown(writer, "")
			}
			settings, conn, err := a.prepareAccountStream(cmd, writer)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			client := brokerstream.New(conn)
			runner := streamrunner.Runner[brokerstream.ServerRequest, investapi.PositionsStreamResponse]{
				Open: func(ctx context.Context) (streamrunner.Session[brokerstream.ServerRequest, investapi.PositionsStreamResponse], error) {
					return client.OpenPositions(ctx, settings.AccountID)
				},
				Watchdog:   streamrunner.DefaultWatchdog,
				IsActivity: brokerstream.PositionsActivity,
				OnLifecycle: func(event streamrunner.LifecycleEvent) error {
					return writer.Write(withAccount(render.LifecycleStreamEvent(event), settings.AccountID))
				},
				OnMessage: func(response *investapi.PositionsStreamResponse) error {
					return writer.Write(withAccount(render.PositionsStreamEvent(response, time.Now()), settings.AccountID))
				},
			}
			return a.runStream(cmd.Context(), writer, settings.AccountID, runner.Run)
		},
	}
}

func (a *app) streamOrdersCmd() *cobra.Command {
	return &cobra.Command{
		Use: "orders", Short: "Stream executed order trades", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			writer := render.NewNDJSONWriter(os.Stdout)
			if cmd.Context().Err() != nil {
				return a.streamShutdown(writer, "")
			}
			settings, conn, err := a.prepareAccountStream(cmd, writer)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			client := brokerstream.New(conn)
			runner := streamrunner.Runner[brokerstream.ServerRequest, investapi.TradesStreamResponse]{
				Open: func(ctx context.Context) (streamrunner.Session[brokerstream.ServerRequest, investapi.TradesStreamResponse], error) {
					return client.OpenOrders(ctx, settings.AccountID)
				},
				Watchdog:   streamrunner.DefaultWatchdog,
				IsActivity: brokerstream.OrdersActivity,
				OnLifecycle: func(event streamrunner.LifecycleEvent) error {
					return writer.Write(withAccount(render.LifecycleStreamEvent(event), settings.AccountID))
				},
				OnMessage: func(response *investapi.TradesStreamResponse) error {
					return writer.Write(withAccount(render.OrdersStreamEvent(response, time.Now()), settings.AccountID))
				},
			}
			return a.runStream(cmd.Context(), writer, settings.AccountID, runner.Run)
		},
	}
}

func (a *app) prepareAccountStream(cmd *cobra.Command, writer *render.NDJSONWriter) (config.Settings, *grpc.ClientConn, error) {
	settings, cerr := a.settings()
	if cerr != nil {
		return config.Settings{}, nil, a.streamFail(writer, cerr, "")
	}
	if cerr := requireAccount(settings); cerr != nil {
		return config.Settings{}, nil, a.streamFail(writer, cerr, settings.AccountID)
	}
	conn, cerr := a.connect(cmd.Context(), settings)
	if cerr != nil {
		return config.Settings{}, nil, a.streamFail(writer, cerr, settings.AccountID)
	}
	return settings, conn, nil
}

func (a *app) runStream(
	ctx context.Context,
	writer *render.NDJSONWriter,
	accountID string,
	run func(context.Context) error,
) error {
	err := run(ctx)
	if err == nil {
		if flushErr := writer.Flush(); flushErr != nil {
			fmt.Fprintf(os.Stderr, "error flushing final stream event: %v\n", flushErr)
			return &exitError{render.ExitInternal}
		}
		return nil
	}
	classified := render.Classify(err, render.CallContext{Phase: transport.PhaseNotSent})
	return a.streamFail(writer, classified, accountID)
}

func (a *app) streamFail(writer *render.NDJSONWriter, cerr *render.CLIError, accountID string) error {
	event := render.NewStreamEvent("error", time.Now(), nil)
	event.AccountID = accountID
	event.Error = cerr.Body()
	if err := writer.Write(event); err != nil {
		fmt.Fprintf(os.Stderr, "error writing stream event: %v\n", err)
	}
	return &exitError{cerr.ExitCode()}
}

func (a *app) streamShutdown(writer *render.NDJSONWriter, accountID string) error {
	event := withAccount(render.LifecycleStreamEvent(streamrunner.LifecycleEvent{
		Type: streamrunner.EventDisconnected, Time: time.Now().UTC(), Reason: "shutdown", Final: true,
	}), accountID)
	if err := writer.Write(event); err != nil {
		fmt.Fprintf(os.Stderr, "error writing final stream event: %v\n", err)
		return &exitError{render.ExitInternal}
	}
	return nil
}

func withAccount(event render.StreamEvent, accountID string) render.StreamEvent {
	if event.AccountID == "" {
		event.AccountID = accountID
	}
	return event
}
