package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	brokerinstruments "tinvest/internal/broker/instruments"
	"tinvest/internal/broker/orders"
	"tinvest/internal/config"
	"tinvest/internal/ledger"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/policy"
	"tinvest/internal/render"
	"tinvest/internal/transport"
	"tinvest/internal/transport/retry"
)

// Ledger intent kinds (plan §10).
const (
	kindOrderPlace   = "order.place"
	kindOrderCancel  = "order.cancel"
	kindOrderReplace = "order.replace"
)

const reconcileCommand = "tinvest orders reconcile"

// placeData is the data block of an `orders place` envelope. Exactly one of
// Order/Async is set on a real placement; DryRun sets Preview+MaxLots instead.
type placeData struct {
	Order   *render.PlaceResultView `json:"order,omitempty"`
	Async   *render.AsyncResultView `json:"async,omitempty"`
	DryRun  bool                    `json:"dry_run,omitempty"`
	Preview *render.PreviewView     `json:"preview,omitempty"`
	MaxLots *render.MaxLotsView     `json:"max_lots,omitempty"`
}

// orderPayload is the token-free request document journaled at Begin (plan §10).
type orderPayload struct {
	AccountID          string `json:"account_id"`
	Endpoint           string `json:"endpoint"`
	InstrumentID       string `json:"instrument_id"`
	OrderID            string `json:"order_id"`
	Direction          string `json:"direction"`
	OrderType          string `json:"order_type"`
	Lots               int64  `json:"lots"`
	Price              string `json:"price,omitempty"`
	TimeInForce        string `json:"time_in_force,omitempty"`
	Async              bool   `json:"async,omitempty"`
	ConfirmMarginTrade bool   `json:"confirm_margin_trade,omitempty"`
}

func (a *app) ordersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "orders",
		Short: "Place, track, cancel, and reconcile orders",
	}
	cmd.AddCommand(
		a.ordersPlaceCmd(),
		a.ordersGetCmd(),
		a.ordersListCmd(),
		a.ordersCancelCmd(),
		a.ordersReplaceCmd(),
		a.ordersPreviewCmd(),
		a.ordersMaxLotsCmd(),
		a.ordersWaitCmd(),
		a.ordersReconcileCmd(),
	)
	return cmd
}

// placeFlags is the flag surface of `orders place`, mirrored by placeInput for
// --input (see that type's doc comment for the JSON schema).
type placeFlags struct {
	instrument         string
	direction          string
	quantity           int64
	orderType          string
	price              string
	tif                string
	orderID            string
	async              bool
	confirmMarginTrade bool
	dryRun             bool
	yes                bool
	input              string
	noCache            bool
}

func (a *app) ordersPlaceCmd() *cobra.Command {
	var f placeFlags
	cmd := &cobra.Command{
		Use:   "place",
		Short: "Place an order (idempotent, journaled)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runPlace(cmd, &f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.instrument, "instrument", "", "instrument id: uid, FIGI, or TICKER@CLASSCODE")
	fl.StringVar(&f.direction, "direction", "", "buy or sell")
	fl.Int64Var(&f.quantity, "quantity", 0, "number of lots (positive)")
	fl.StringVar(&f.orderType, "type", "", "limit, market, or bestprice")
	fl.StringVar(&f.price, "price", "", "limit price as a decimal string (required for limit)")
	fl.StringVar(&f.tif, "tif", "", "time in force: day, ioc, or fok")
	fl.StringVar(&f.orderID, "order-id", "", "client idempotency key (UUID); generated if omitted")
	fl.BoolVar(&f.async, "async", false, "place asynchronously (PostOrderAsync)")
	fl.BoolVar(&f.confirmMarginTrade, "confirm-margin-trade", false, "confirm an order that may create an uncovered position")
	fl.BoolVar(&f.dryRun, "dry-run", false, "validate and preview only; place nothing")
	fl.BoolVar(&f.yes, "yes", false, "confirm the mutation (accepted for symmetry; no interactive prompt)")
	fl.StringVar(&f.input, "input", "", "read the full request as JSON from a file or - for stdin")
	fl.BoolVar(&f.noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

// resolvedPlace is a place request after flag/JSON parsing and enum resolution,
// before any network call. Price is nil for market/bestprice.
type resolvedPlace struct {
	instrument         string
	direction          investapi.OrderDirection
	orderType          investapi.OrderType
	lots               int64
	price              *investapi.Quotation
	priceStr           string
	tif                investapi.TimeInForceType
	tifStr             string
	orderID            string
	async              bool
	confirmMarginTrade bool
	dryRun             bool
}

// runPlace executes the safe-placement vertical slice (plan §9), in the exact
// order the reliability model binds: parse+validate locally -> require account
// -> policy (local) -> connect -> resolve -> policy (resolved) + price
// increment -> [dry-run: preview+maxlots] -> ledger Begin -> SendStarted ->
// PostOrder(idempotent) -> Confirmed/Rejected -> envelope.
func (a *app) runPlace(cmd *cobra.Command, f *placeFlags) error {
	start := time.Now()
	settings, cerr := a.settings()
	mode := render.Mode(settings.Output, os.Stdout)
	if cerr != nil {
		return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
	}
	metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }

	// Resolve the request from flags or JSON stdin/file (mutually exclusive).
	rp, cerr := resolvePlaceRequest(cmd, f)
	if cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}

	// Local shape validation — no instrument data, no token needed.
	if err := orders.ValidateBasics(rp.orderType, rp.lots, rp.price); err != nil {
		return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
	}

	// A real (non-dry-run) mutation requires an account.
	if !rp.dryRun {
		if cerr := requireAccount(settings); cerr != nil {
			return a.fail(mode, cerr, metaNoNet())
		}
	}

	// Load and apply local policy (kill switch, market opt-in, lot cap) before
	// any network call, so a guardrail breach is exit 2 without a token.
	pol, err := policy.Load(settings.PolicyFile)
	if err != nil {
		return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
	}
	localIntent := policy.OrderIntent{
		Direction: rp.direction,
		OrderType: rp.orderType,
		Lots:      rp.lots,
		Price:     absolutePolicyPrice(rp.price),
		RawID:     rp.instrument,
	}
	if v := pol.CheckLocal(localIntent); v != nil {
		return a.fail(mode, render.PolicyError(v.Message, v.Details), metaNoNet())
	}

	conn, cerr := a.connect(cmd.Context(), settings)
	if cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}
	defer func() { _ = conn.Close() }()

	// Resolve the instrument (network read).
	inst, cerr, trackingID := a.resolveOne(cmd.Context(), conn, rp.instrument, f.noCache)
	if cerr != nil {
		return a.fail(mode, cerr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
	}
	uid := inst.GetUid()

	// Resolved policy checks (allowlist, notional) and the price-increment check.
	resolvedIntent := localIntent
	resolvedIntent.UID = uid
	resolvedIntent.FIGI = inst.GetFigi()
	resolvedIntent.Ticker = inst.GetTicker()
	resolvedIntent.ClassCode = inst.GetClassCode()
	resolvedIntent.LotSize = inst.GetLot()
	resolvedIntent.Currency = inst.GetCurrency()
	if v := pol.CheckResolved(resolvedIntent); v != nil {
		return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
	}
	if err := orders.ValidatePriceIncrement(rp.price, inst.GetMinPriceIncrement()); err != nil {
		return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
	}

	cl := orders.New(conn)

	// --dry-run: preview + max-lots, no ledger entry, no order (plan §8/§9).
	if rp.dryRun {
		return a.runDryRun(cmd.Context(), cl, settings, uid, rp, start, mode)
	}

	// Enforce the open-order cap (needs a read) before placing.
	if pol != nil && pol.MaxOpenOrders > 0 {
		ctx, info := transport.WithCallInfo(cmd.Context())
		open, err := cl.List(ctx, settings.AccountID)
		if err != nil {
			return a.fail(mode, render.Classify(err, callContext(info, false)), render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start)))
		}
		if v := pol.CheckOpenOrders(len(open)); v != nil {
			return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start)))
		}
	}

	led, err := a.openLedger()
	if err != nil {
		return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}, render.NewMeta(settings.AccountID, "", time.Since(start)))
	}
	defer func() { _ = led.Close() }()

	intent := ledger.Intent{
		IntentID:  rp.orderID,
		Kind:      kindOrderPlace,
		AccountID: settings.AccountID,
		Profile:   settings.Profile,
		Attempt:   1,
		OrderID:   rp.orderID,
		Payload: orderPayload{
			AccountID:          settings.AccountID,
			Endpoint:           settings.Endpoint,
			InstrumentID:       uid,
			OrderID:            rp.orderID,
			Direction:          rp.direction.String(),
			OrderType:          rp.orderType.String(),
			Lots:               rp.lots,
			Price:              rp.priceStr,
			TimeInForce:        rp.tifStr,
			Async:              rp.async,
			ConfirmMarginTrade: rp.confirmMarginTrade,
		},
	}
	params := orders.PlaceParams{
		AccountID:          settings.AccountID,
		InstrumentID:       uid,
		OrderID:            rp.orderID,
		Direction:          rp.direction,
		OrderType:          rp.orderType,
		Lots:               rp.lots,
		Price:              rp.price,
		TimeInForce:        rp.tif,
		ConfirmMarginTrade: rp.confirmMarginTrade,
	}

	// Re-check the kill switch immediately before the send: the operator may
	// have engaged it during the resolve / open-order lookups above, and it must
	// take effect before the order actually goes out (finding F11). This is the
	// last point before the ledger's send-started write, so a hit here leaves no
	// spurious unresolved entry.
	if v := pol.CheckKillSwitch(); v != nil {
		return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
	}

	out, cerr := placeExec(cmd.Context(), cl, led, intent, params, rp.async)
	meta := render.NewMeta(settings.AccountID, out.trackingID(), time.Since(start))
	if cerr != nil {
		return a.fail(mode, cerr, meta)
	}
	data := placeData{}
	if rp.async {
		v := render.AsyncResult(out.Async, rp.orderID, orders.Lifecycle(out.Async.GetExecutionReportStatus()))
		data.Async = &v
	} else {
		v := render.PlaceResult(out.Sync, rp.orderID, orders.Lifecycle(out.Sync.GetExecutionReportStatus()))
		data.Order = &v
	}
	if mode == "table" {
		return placeTable(os.Stdout, data)
	}
	return render.WriteJSON(os.Stdout, render.Success(data, meta))
}

// placeOutcome carries whichever response shape the placement produced plus the
// transport observations for that call.
type placeOutcome struct {
	Sync  *investapi.PostOrderResponse
	Async *investapi.PostOrderAsyncResponse
	Info  *transport.CallInfo
}

func (o *placeOutcome) trackingID() string {
	if o == nil || o.Info == nil {
		return ""
	}
	return o.Info.TrackingID()
}

// placeExec is the crash-safe placement sequence (plan §9), decoupled from
// cobra for testing. It journals Begin -> SendStarted before the send, marks
// the context idempotent so a timed-out PostOrder retries under the same
// order_id, then records Confirmed or Rejected. On an unconfirmable outcome
// (phase sent_unconfirmed) it deliberately leaves the ledger entry unresolved
// and returns an exit-7 error carrying the order_id and a reconcile hint.
func placeExec(cmdCtx context.Context, cl orders.Client, led *ledger.Ledger, intent ledger.Intent, p orders.PlaceParams, async bool) (*placeOutcome, *render.CLIError) {
	entry, err := led.Begin(intent)
	if err != nil {
		return nil, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("ledger begin: %v", err)}
	}
	if err := entry.SendStarted(); err != nil {
		return nil, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("ledger send-started: %v", err)}
	}

	ctx, info := transport.WithCallInfo(retry.Idempotent(cmdCtx))
	out := &placeOutcome{Info: info}
	var respErr error
	if async {
		out.Async, respErr = cl.PlaceAsync(ctx, p)
	} else {
		out.Sync, respErr = cl.Place(ctx, p)
	}

	cc := callContext(info, true)
	if respErr != nil {
		cerr := render.Classify(respErr, cc)
		if cerr.Code == render.CodeUnconfirmed {
			// Leave the entry at send-started (Unresolved): the order may have
			// reached the broker. Attach the reconcile hint (plan §9).
			cerr.ReconcileHint = &render.ReconcileHint{OrderID: p.OrderID, Command: reconcileCommand}
			return out, cerr
		}
		// A confirmed error (server status) is a definitive outcome: record it.
		_ = entry.Rejected(respErr)
		return out, cerr
	}

	execStatus := out.executionStatus()
	if orders.IsRejected(execStatus) {
		msg := out.rejectMessage()
		_ = entry.Rejected(errors.New(msg))
		return out, &render.CLIError{
			Code:       render.CodeBrokerRejected,
			Message:    msg,
			Phase:      transport.PhaseConfirmed.String(),
			TrackingID: info.TrackingID(),
		}
	}

	_ = entry.Confirmed(ledger.Result{
		OrderID:    out.exchangeOrderID(),
		TrackingID: info.TrackingID(),
	})
	return out, nil
}

func (o *placeOutcome) executionStatus() investapi.OrderExecutionReportStatus {
	if o.Sync != nil {
		return o.Sync.GetExecutionReportStatus()
	}
	if o.Async != nil {
		return o.Async.GetExecutionReportStatus()
	}
	return investapi.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_UNSPECIFIED
}

func (o *placeOutcome) exchangeOrderID() string {
	if o.Sync != nil {
		return o.Sync.GetOrderId()
	}
	return ""
}

func (o *placeOutcome) rejectMessage() string {
	if o.Sync != nil && o.Sync.GetMessage() != "" {
		return o.Sync.GetMessage()
	}
	return "order rejected by broker"
}

func (a *app) runDryRun(cmdCtx context.Context, cl orders.Client, settings config.Settings, uid string, rp resolvedPlace, start time.Time, mode string) error {
	// A preview needs an account too, but we do not require --account to fail
	// hard here: preview/max-lots without an account simply return whatever the
	// broker gives. We pass settings.AccountID through as-is.
	ctx, info := transport.WithCallInfo(cmdCtx)
	preview, err := cl.Preview(ctx, orders.PreviewParams{
		AccountID:    settings.AccountID,
		InstrumentID: uid,
		Direction:    rp.direction,
		Lots:         rp.lots,
		Price:        rp.price,
	})
	if err != nil {
		return a.fail(mode, render.Classify(err, callContext(info, false)), render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start)))
	}
	ctx2, info2 := transport.WithCallInfo(cmdCtx)
	maxLots, err := cl.MaxLots(ctx2, settings.AccountID, uid, rp.price)
	if err != nil {
		return a.fail(mode, render.Classify(err, callContext(info2, false)), render.NewMeta(settings.AccountID, info2.TrackingID(), time.Since(start)))
	}

	pv := render.Preview(preview)
	mv := render.MaxLots(maxLots)
	data := placeData{DryRun: true, Preview: &pv, MaxLots: &mv}
	meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
	if mode == "table" {
		return placeTable(os.Stdout, data)
	}
	return render.WriteJSON(os.Stdout, render.Success(data, meta))
}

// resolvePlaceRequest turns flags or JSON input into a resolvedPlace, enforcing
// that --input and the order-shaping flags are mutually exclusive (plan §7).
func resolvePlaceRequest(cmd *cobra.Command, f *placeFlags) (resolvedPlace, *render.CLIError) {
	fl := cmd.Flags()
	if f.input != "" {
		for _, name := range []string{"instrument", "direction", "quantity", "type", "price", "tif", "order-id", "async", "confirm-margin-trade"} {
			if fl.Changed(name) {
				return resolvedPlace{}, render.UsageError("--input is mutually exclusive with order flags (e.g. --" + name + ")")
			}
		}
		return resolvePlaceInput(f.input)
	}
	return buildPlace(placeInput{
		Instrument:         f.instrument,
		Direction:          f.direction,
		Quantity:           f.quantity,
		Type:               f.orderType,
		Price:              f.price,
		TIF:                f.tif,
		OrderID:            f.orderID,
		Async:              f.async,
		ConfirmMarginTrade: f.confirmMarginTrade,
		DryRun:             f.dryRun,
	})
}

// placeInput is the JSON document accepted by `orders place --input`. It mirrors
// the flag surface exactly and is the replayable, shell-safe request form
// (plan §7). Schema (schema_version aligns with the CLI output contract):
//
//	{
//	  "instrument": "<uid | FIGI | TICKER@CLASSCODE>",  // required
//	  "direction":  "buy" | "sell",                      // required
//	  "quantity":   <int lots > 0>,                      // required
//	  "type":       "limit" | "market" | "bestprice",    // required
//	  "price":      "<decimal string>",                  // required for limit
//	  "tif":        "day" | "ioc" | "fok",               // optional
//	  "order_id":   "<uuid>",                            // optional; generated
//	  "async":      <bool>,                              // optional
//	  "confirm_margin_trade": <bool>,                     // optional
//	  "dry_run":    <bool>                               // optional
//	}
//
// Unknown fields are rejected so a misspelled key fails loudly rather than
// silently dropping a safety-relevant value.
type placeInput struct {
	Instrument         string `json:"instrument"`
	Direction          string `json:"direction"`
	Quantity           int64  `json:"quantity"`
	Type               string `json:"type"`
	Price              string `json:"price,omitempty"`
	TIF                string `json:"tif,omitempty"`
	OrderID            string `json:"order_id,omitempty"`
	Async              bool   `json:"async,omitempty"`
	ConfirmMarginTrade bool   `json:"confirm_margin_trade,omitempty"`
	DryRun             bool   `json:"dry_run,omitempty"`
}

func resolvePlaceInput(source string) (resolvedPlace, *render.CLIError) {
	var reader io.Reader
	if source == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(source)
		if err != nil {
			return resolvedPlace{}, render.UsageError(fmt.Sprintf("open input %s: %v", source, err))
		}
		defer func() { _ = file.Close() }()
		reader = file
	}
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	var in placeInput
	if err := dec.Decode(&in); err != nil {
		return resolvedPlace{}, render.UsageError(fmt.Sprintf("invalid JSON input: %v", err))
	}
	return buildPlace(in)
}

// buildPlace validates and resolves a placeInput (from flags or JSON) into a
// resolvedPlace: it classifies the instrument id, parses the enums and price,
// and generates a client order_id when none was supplied (plan §9 — the key is
// persisted to the ledger before the send, so a crash cannot re-issue it under
// a new key).
func buildPlace(in placeInput) (resolvedPlace, *render.CLIError) {
	if _, err := brokerinstruments.Classify(in.Instrument); err != nil {
		return resolvedPlace{}, render.UsageError(err.Error())
	}
	direction, err := orders.Direction(in.Direction)
	if err != nil {
		return resolvedPlace{}, render.UsageError(err.Error())
	}
	orderType, err := orders.Type(in.Type)
	if err != nil {
		return resolvedPlace{}, render.UsageError(err.Error())
	}
	tif, err := orders.TimeInForce(in.TIF)
	if err != nil {
		return resolvedPlace{}, render.UsageError(err.Error())
	}

	var price *investapi.Quotation
	priceStr := strings.TrimSpace(in.Price)
	if priceStr != "" {
		q, err := render.ParseQuotation(priceStr)
		if err != nil {
			return resolvedPlace{}, render.UsageError(fmt.Sprintf("invalid --price %q: %v", in.Price, err))
		}
		price = q
	}

	orderID := strings.TrimSpace(in.OrderID)
	if orderID == "" {
		generated, err := newOrderID()
		if err != nil {
			return resolvedPlace{}, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("generate order id: %v", err)}
		}
		orderID = generated
	}
	if err := validateOrderID(orderID); err != nil {
		return resolvedPlace{}, render.UsageError(err.Error())
	}

	return resolvedPlace{
		instrument:         in.Instrument,
		direction:          direction,
		orderType:          orderType,
		lots:               in.Quantity,
		price:              price,
		priceStr:           priceStr,
		tif:                tif,
		tifStr:             in.TIF,
		orderID:            orderID,
		async:              in.Async,
		confirmMarginTrade: in.ConfirmMarginTrade,
		dryRun:             in.DryRun,
	}, nil
}

// ---- get / list / cancel / replace / preview / max-lots ----

type orderGetData struct {
	Order render.OrderStateView `json:"order"`
}

func (a *app) ordersGetCmd() *cobra.Command {
	var byRequestID bool
	cmd := &cobra.Command{
		Use:   "get <order-id>",
		Short: "Order state by exchange order id (or --request-id for the client key)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			state, err := orders.New(conn).Get(ctx, settings.AccountID, args[0], byRequestID)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			view := render.OrderState(state, orders.Lifecycle(state.GetExecutionReportStatus()))
			if mode == "table" {
				return render.OrderStatesTable(os.Stdout, []render.OrderStateView{view})
			}
			return render.WriteJSON(os.Stdout, render.Success(orderGetData{Order: view}, meta))
		},
	}
	cmd.Flags().BoolVar(&byRequestID, "request-id", false, "interpret the id as the client idempotency key, not the exchange id")
	return cmd
}

type ordersListData struct {
	Orders []render.OrderStateView `json:"orders"`
}

func (a *app) ordersListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List active orders on the account",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			list, err := orders.New(conn).List(ctx, settings.AccountID)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := make([]render.OrderStateView, 0, len(list))
			for _, s := range list {
				views = append(views, render.OrderState(s, orders.Lifecycle(s.GetExecutionReportStatus())))
			}
			if mode == "table" {
				return render.OrderStatesTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(ordersListData{Orders: views}, meta))
		},
	}
}

type cancelData struct {
	OrderID string `json:"order_id"`
	Time    string `json:"time,omitempty"`
}

func (a *app) ordersCancelCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel <order-id>",
		Short: "Cancel an active order (idempotent)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			pol, err := policy.Load(settings.PolicyFile)
			if err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if v := pol.CheckKillSwitch(); v != nil {
				return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			// Re-check the kill switch immediately before the send (finding F11).
			if v := pol.CheckKillSwitch(); v != nil {
				return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			// CancelOrder is convergent when repeated, so retry is safe.
			ctx, info := transport.WithCallInfo(retry.Idempotent(cmd.Context()))
			resp, err := orders.New(conn).Cancel(ctx, settings.AccountID, args[0])
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				cerr := render.Classify(err, callContext(info, true))
				command := fmt.Sprintf("tinvest orders get %s", args[0])
				return a.fail(mode, addCancelReconcileHint(cerr, args[0], command), meta)
			}
			data := cancelData{OrderID: args[0], Time: render.Timestamp(resp.GetTime())}
			if mode == "table" {
				return render.Table(os.Stdout, []string{"ORDER_ID", "CANCELLED_AT"}, [][]string{{data.OrderID, data.Time}})
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
	return cmd
}

func (a *app) ordersReplaceCmd() *cobra.Command {
	var quantity int64
	var price string
	var confirmMarginTrade bool
	cmd := &cobra.Command{
		Use:   "replace <order-id>",
		Short: "Replace an active order's price and/or quantity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if quantity <= 0 {
				return a.fail(mode, render.UsageError("replace requires a positive --quantity"), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			var priceQ *investapi.Quotation
			if strings.TrimSpace(price) != "" {
				q, err := render.ParseQuotation(price)
				if err != nil {
					return a.fail(mode, render.UsageError(fmt.Sprintf("invalid --price %q: %v", price, err)), render.NewMeta(settings.AccountID, "", time.Since(start)))
				}
				priceQ = q
			}
			pol, err := policy.Load(settings.PolicyFile)
			if err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if v := pol.CheckKillSwitch(); v != nil {
				return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if v := pol.CheckLocal(policy.OrderIntent{Lots: quantity, Price: priceQ}); v != nil {
				return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()
			cl := orders.New(conn)

			stateCtx, stateInfo := transport.WithCallInfo(cmd.Context())
			state, err := cl.Get(stateCtx, settings.AccountID, args[0], false)
			if err != nil {
				meta := render.NewMeta(settings.AccountID, stateInfo.TrackingID(), time.Since(start))
				return a.fail(mode, render.Classify(err, callContext(stateInfo, false)), meta)
			}
			instrumentID := replacementInstrumentID(state)
			if instrumentID == "" {
				return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: "broker order state contains no instrument identifier"}, render.NewMeta(settings.AccountID, stateInfo.TrackingID(), time.Since(start)))
			}
			inst, cerr, trackingID := a.resolveOne(cmd.Context(), conn, instrumentID, false)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}
			if v := replacementPolicyViolation(pol, quantity, priceQ, state, inst); v != nil {
				return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, stateInfo.TrackingID(), time.Since(start)))
			}
			if err := orders.ValidatePriceIncrement(priceQ, inst.GetMinPriceIncrement()); err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}

			// ReplaceOrder carries its own idempotency key. Its dedup retention
			// is the same as PostOrder, so a fresh key is generated per attempt
			// by the caller; auto-retry stays off (no Idempotent marker) because
			// a replace is not convergent on repeat.
			key, err := newOrderID()
			if err != nil {
				return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			led, err := a.openLedger()
			if err != nil {
				return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = led.Close() }()

			// Re-check the kill switch just before the send: the state lookup
			// and instrument resolution above are a window in which the operator
			// may have engaged it (finding F11).
			if v := pol.CheckKillSwitch(); v != nil {
				return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}

			entry, err := led.Begin(ledger.Intent{
				IntentID: key, Kind: kindOrderReplace, AccountID: settings.AccountID,
				Profile: settings.Profile, Attempt: 1, OrderID: key,
				Payload: map[string]any{
					"endpoint": settings.Endpoint, "replaces": args[0], "quantity": quantity,
					"price": price, "confirm_margin_trade": confirmMarginTrade,
				},
			})
			if err != nil {
				return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if err := entry.SendStarted(); err != nil {
				return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("ledger send-started: %v", err)}, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}

			ctx, info := transport.WithCallInfo(cmd.Context())
			resp, err := cl.Replace(ctx, orders.ReplaceParams{
				AccountID: settings.AccountID, OrderID: args[0], IdempotencyKey: key,
				Lots: quantity, Price: priceQ, ConfirmMarginTrade: confirmMarginTrade,
			})
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				cerr := render.Classify(err, callContext(info, true))
				if cerr.Code == render.CodeUnconfirmed {
					cerr.ReconcileHint = &render.ReconcileHint{OrderID: key, Command: reconcileCommand}
					return a.fail(mode, cerr, meta)
				}
				_ = entry.Rejected(err)
				return a.fail(mode, cerr, meta)
			}
			_ = entry.Confirmed(ledger.Result{OrderID: resp.GetOrderId(), TrackingID: info.TrackingID()})
			view := render.PlaceResult(resp, key, orders.Lifecycle(resp.GetExecutionReportStatus()))
			if mode == "table" {
				return placeTable(os.Stdout, placeData{Order: &view})
			}
			return render.WriteJSON(os.Stdout, render.Success(placeData{Order: &view}, meta))
		},
	}
	cmd.Flags().Int64Var(&quantity, "quantity", 0, "new number of lots (positive)")
	cmd.Flags().StringVar(&price, "price", "", "new limit price as a decimal string")
	cmd.Flags().BoolVar(&confirmMarginTrade, "confirm-margin-trade", false, "confirm a replacement that may create an uncovered position")
	return cmd
}

func replacementInstrumentID(state *investapi.OrderState) string {
	if state == nil {
		return ""
	}
	if state.GetInstrumentUid() != "" {
		return state.GetInstrumentUid()
	}
	if state.GetFigi() != "" {
		return state.GetFigi()
	}
	if state.GetTicker() != "" && state.GetClassCode() != "" {
		return state.GetTicker() + "@" + state.GetClassCode()
	}
	return ""
}

func replacementPolicyViolation(
	pol *policy.Policy,
	lots int64,
	requestedPrice *investapi.Quotation,
	state *investapi.OrderState,
	inst *investapi.Instrument,
) *policy.Violation {
	price := requestedPrice
	if price == nil && state != nil {
		if state.GetInitialSecurityPrice() != nil {
			price = &investapi.Quotation{
				Units: state.GetInitialSecurityPrice().GetUnits(),
				Nano:  state.GetInitialSecurityPrice().GetNano(),
			}
		}
	}
	intent := policy.OrderIntent{Lots: lots, Price: absolutePolicyPrice(price)}
	if state != nil {
		intent.Direction = state.GetDirection()
		intent.OrderType = state.GetOrderType()
		intent.RawID = replacementInstrumentID(state)
	}
	if v := pol.CheckLocal(intent); v != nil {
		return v
	}
	if inst != nil {
		intent.UID = inst.GetUid()
		intent.FIGI = inst.GetFigi()
		intent.Ticker = inst.GetTicker()
		intent.ClassCode = inst.GetClassCode()
		intent.LotSize = inst.GetLot()
		intent.Currency = inst.GetCurrency()
	}
	return pol.CheckResolved(intent)
}

// absolutePolicyPrice preserves the signed broker request while making a
// notional cap apply to the magnitude of futures prices that may be negative.
func absolutePolicyPrice(price *investapi.Quotation) *investapi.Quotation {
	if price == nil || (price.GetUnits() >= 0 && price.GetNano() >= 0) {
		return price
	}
	return &investapi.Quotation{Units: -price.GetUnits(), Nano: -price.GetNano()}
}

type previewData struct {
	Preview render.PreviewView `json:"preview"`
}

func (a *app) ordersPreviewCmd() *cobra.Command {
	var f placeFlags
	cmd := &cobra.Command{
		Use:   "preview",
		Short: "Pre-trade cost and commission (GetOrderPrice), places nothing",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			rp, cerr := buildPlace(placeInput{
				Instrument: f.instrument, Direction: f.direction, Quantity: f.quantity,
				Type: f.orderType, Price: f.price,
			})
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			if err := orders.ValidateBasics(rp.orderType, rp.lots, rp.price); err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			inst, cerr, trackingID := a.resolveOne(cmd.Context(), conn, rp.instrument, f.noCache)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}
			ctx, info := transport.WithCallInfo(cmd.Context())
			resp, err := orders.New(conn).Preview(ctx, orders.PreviewParams{
				AccountID: settings.AccountID, InstrumentID: inst.GetUid(),
				Direction: rp.direction, Lots: rp.lots, Price: rp.price,
			})
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			data := previewData{Preview: render.Preview(resp)}
			if mode == "table" {
				return placeTable(os.Stdout, placeData{Preview: &data.Preview})
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.instrument, "instrument", "", "instrument id: uid, FIGI, or TICKER@CLASSCODE")
	fl.StringVar(&f.direction, "direction", "", "buy or sell")
	fl.Int64Var(&f.quantity, "quantity", 0, "number of lots (positive)")
	fl.StringVar(&f.orderType, "type", "limit", "limit, market, or bestprice")
	fl.StringVar(&f.price, "price", "", "price as a decimal string (required for limit)")
	fl.BoolVar(&f.noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

type maxLotsData struct {
	MaxLots render.MaxLotsView `json:"max_lots"`
}

func (a *app) ordersMaxLotsCmd() *cobra.Command {
	var instrument, price string
	var noCache bool
	cmd := &cobra.Command{
		Use:   "max-lots",
		Short: "Maximum tradable lots for an instrument (GetMaxLots)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := validateInstrumentIDs(instrument); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			var priceQ *investapi.Quotation
			if strings.TrimSpace(price) != "" {
				q, err := render.ParseQuotation(price)
				if err != nil {
					return a.fail(mode, render.UsageError(fmt.Sprintf("invalid --price %q: %v", price, err)), render.NewMeta(settings.AccountID, "", time.Since(start)))
				}
				priceQ = q
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			inst, cerr, trackingID := a.resolveOne(cmd.Context(), conn, instrument, noCache)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
			}
			ctx, info := transport.WithCallInfo(cmd.Context())
			resp, err := orders.New(conn).MaxLots(ctx, settings.AccountID, inst.GetUid(), priceQ)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			data := maxLotsData{MaxLots: render.MaxLots(resp)}
			if mode == "table" {
				return placeTable(os.Stdout, placeData{MaxLots: &data.MaxLots})
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
	cmd.Flags().StringVar(&instrument, "instrument", "", "instrument id: uid, FIGI, or TICKER@CLASSCODE")
	cmd.Flags().StringVar(&price, "price", "", "price as a decimal string (refines buy limits)")
	cmd.Flags().BoolVar(&noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

// ---- wait / reconcile ----

func (a *app) ordersWaitCmd() *cobra.Command {
	var timeout time.Duration
	var byRequestID bool
	cmd := &cobra.Command{
		Use:   "wait <order-id>",
		Short: "Block until an order reaches a terminal state (filled/cancelled/rejected)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			settings, cerr := a.settings()
			mode := render.Mode(settings.Output, os.Stdout)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
			}
			if cerr := requireAccount(settings); cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			state, info, cerr := waitFlow(cmd.Context(), orders.New(conn), settings.AccountID, args[0], byRequestID, time.Second, timeout)
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if cerr != nil {
				return a.fail(mode, cerr, meta)
			}
			view := render.OrderState(state, orders.Lifecycle(state.GetExecutionReportStatus()))
			if mode == "table" {
				return render.OrderStatesTable(os.Stdout, []render.OrderStateView{view})
			}
			return render.WriteJSON(os.Stdout, render.Success(orderGetData{Order: view}, meta))
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 60*time.Second, "give up after this long")
	cmd.Flags().BoolVar(&byRequestID, "request-id", false, "interpret the id as the client idempotency key")
	return cmd
}

// waitFlow polls GetOrderState every interval until the order is terminal or
// the timeout elapses (plan §9). It returns the last observed CallInfo so the
// caller can attach a tracking id even on timeout.
func waitFlow(parent context.Context, cl orders.Client, accountID, orderID string, byRequestID bool, interval, timeout time.Duration) (*investapi.OrderState, *transport.CallInfo, *render.CLIError) {
	deadline := time.Now().Add(timeout)
	var lastInfo *transport.CallInfo
	for {
		ctx, info := transport.WithCallInfo(parent)
		lastInfo = info
		state, err := cl.Get(ctx, accountID, orderID, byRequestID)
		if err != nil {
			return nil, info, render.Classify(err, callContext(info, false))
		}
		if orders.IsTerminal(state.GetExecutionReportStatus()) {
			return state, info, nil
		}
		if time.Now().After(deadline) || time.Until(deadline) <= 0 {
			return nil, info, &render.CLIError{
				Code:    render.CodeNetwork,
				Message: fmt.Sprintf("order %s not terminal within %s (last lifecycle: %s)", orderID, timeout, orders.Lifecycle(state.GetExecutionReportStatus())),
				Phase:   transport.PhaseConfirmed.String(),
			}
		}
		select {
		case <-parent.Done():
			return nil, lastInfo, &render.CLIError{Code: render.CodeNetwork, Message: parent.Err().Error()}
		case <-time.After(interval):
		}
	}
}

type reconcileData struct {
	Outcomes           []render.ReconcileOutcomeView `json:"outcomes"`
	ForeignIntentCount int                           `json:"foreign_intent_count,omitempty"`
	ForeignIntentHint  string                        `json:"foreign_intent_hint,omitempty"`
}

type reconcileTarget struct {
	Profile  string
	Endpoint string
}

type reconcileOptions struct {
	// SyncNotFoundDelay is how long to wait before the confirming re-check of a
	// synchronous intent that first read NOT_FOUND. Async intents are never
	// closed on NOT_FOUND, so this does not apply to them.
	SyncNotFoundDelay time.Duration
}

const syncReconcileNotFoundDelay = 2 * time.Second

func newReconcileData(outcomes []render.ReconcileOutcomeView, foreignHint string) reconcileData {
	data := reconcileData{Outcomes: outcomes}
	for _, outcome := range outcomes {
		if outcome.Outcome == "foreign" {
			data.ForeignIntentCount++
		}
	}
	if data.ForeignIntentCount > 0 {
		data.ForeignIntentHint = foreignHint
	}
	return data
}

func (a *app) ordersReconcileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile",
		Short: "Resolve every unconfirmed regular-order intent in the journal against the broker",
		Args:  cobra.NoArgs,
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

			led, err := a.openLedger()
			if err != nil {
				return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = led.Close() }()

			outcomes, cerr := reconcileFlowForTarget(
				cmd.Context(), orders.New(conn), led,
				reconcileTarget{Profile: settings.Profile, Endpoint: settings.Endpoint},
				reconcileOptions{SyncNotFoundDelay: syncReconcileNotFoundDelay},
			)
			meta := render.NewMeta(settings.AccountID, "", time.Since(start))
			if cerr != nil {
				return a.fail(mode, cerr, meta)
			}
			if mode == "table" {
				return reconcileTable(os.Stdout, outcomes)
			}
			return render.WriteJSON(os.Stdout, render.Success(newReconcileData(outcomes, stopReconcileCommand), meta))
		},
	}
}

// reconcileFlowForTarget resolves regular order placement/replacement intents
// only. Foreign intent kinds and intents recorded for another profile or
// endpoint are reported and left untouched.
//
// NOT_FOUND is handled differently by placement mode. A synchronous PostOrder is
// queryable immediately once accepted, so a persistent NOT_FOUND is meaningful:
// it is re-checked once after a short delay (to absorb a transient miss) and then
// closed as not-placed. A PostOrderAsync order is NOT queryable via
// GetOrderState until the exchange assigns its id — which arrives over the orders
// stream with no documented upper bound — so NOT_FOUND must never close it as
// not-placed. Instead the day's order list is scanned for the exchange order by
// order_request_id; if it is not there either, the intent is left unresolved for
// a later run (see reconcileAsyncNotFound).
func reconcileFlowForTarget(
	ctx context.Context,
	cl orders.Client,
	led *ledger.Ledger,
	target reconcileTarget,
	options reconcileOptions,
) ([]render.ReconcileOutcomeView, *render.CLIError) {
	entries, err := led.Unresolved()
	if err != nil {
		return nil, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("read journal: %v", err)}
	}
	outcomes := make([]render.ReconcileOutcomeView, 0, len(entries))
	for _, e := range entries {
		out := render.ReconcileOutcomeView{
			IntentID:      e.IntentID(),
			ClientOrderID: e.OrderID(),
			AccountID:     e.AccountID(),
		}
		if e.Kind() != kindOrderPlace && e.Kind() != kindOrderReplace {
			out.Outcome = "foreign"
			out.Error = foreignIntentMessage(e.Kind(), stopReconcileCommand)
			outcomes = append(outcomes, out)
			continue
		}
		if outcome, message := reconcileTargetMismatch(e, target); message != "" {
			out.Outcome = outcome
			out.Error = message
			outcomes = append(outcomes, out)
			continue
		}
		if e.OrderID() == "" || e.AccountID() == "" {
			// Nothing to look the order up by; leave it for a human.
			out.Outcome = "indeterminate"
			out.Error = "missing account or order id in journal entry"
			outcomes = append(outcomes, out)
			continue
		}
		var payload orderPayload
		if err := json.Unmarshal(e.Payload(), &payload); err != nil {
			out.Outcome = "indeterminate"
			out.Error = fmt.Sprintf("unreadable journal payload: %v", err)
			outcomes = append(outcomes, out)
			continue
		}
		cctx, info := transport.WithCallInfo(ctx)
		state, err := cl.Get(cctx, e.AccountID(), e.OrderID(), true)
		if err != nil {
			if status.Code(err) != codes.NotFound {
				out.Outcome = "error"
				out.Error = render.Classify(err, callContext(info, false)).Message
				outcomes = append(outcomes, out)
				continue
			}
			// NOT_FOUND. Async orders are not queryable until exchange acceptance,
			// so they are never closed here; sync orders get a confirming re-check.
			if payload.Async {
				reconcileAsyncNotFound(ctx, cl, e, &out)
				outcomes = append(outcomes, out)
				continue
			}
			state, info, err = recheckNotFound(ctx, cl, e, options.SyncNotFoundDelay)
			if err != nil && status.Code(err) != codes.NotFound {
				out.Outcome = "indeterminate"
				out.Error = fmt.Sprintf("one NOT_FOUND was observed, but confirmation failed; retry reconcile later: %s", render.Classify(err, callContext(info, false)).Message)
				outcomes = append(outcomes, out)
				continue
			}
			if err != nil {
				out.Outcome = "not-placed"
				_ = e.Reconciled(ledger.Result{Error: "not-placed"})
				outcomes = append(outcomes, out)
				continue
			}
			// The re-check found it after all.
		}
		setPlacedReconcileOutcome(&out, state)
		_ = e.Reconciled(ledger.Result{OrderID: state.GetOrderId(), TrackingID: info.TrackingID()})
		outcomes = append(outcomes, out)
	}
	return outcomes, nil
}

// recheckNotFound waits out a short delay and re-queries GetOrderState, used to
// confirm a synchronous intent's NOT_FOUND is persistent (not a transient miss)
// before it is closed as not-placed.
func recheckNotFound(
	ctx context.Context,
	cl orders.Client,
	entry *ledger.Entry,
	delay time.Duration,
) (*investapi.OrderState, *transport.CallInfo, error) {
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			_, info := transport.WithCallInfo(ctx)
			return nil, info, ctx.Err()
		case <-timer.C:
		}
	}
	cctx, info := transport.WithCallInfo(ctx)
	state, err := cl.Get(cctx, entry.AccountID(), entry.OrderID(), true)
	return state, info, err
}

// reconcileAsyncNotFound resolves a GetOrderState NOT_FOUND for an async
// (PostOrderAsync) intent. Per the API, such an order is not queryable via
// GetOrderState until the exchange assigns its id (delivered over the orders
// stream, with no documented upper bound), so NOT_FOUND must never close it as
// not-placed. It scans the day's order list for the exchange order by
// order_request_id; if found the intent is closed as placed, otherwise it is
// left UNRESOLVED (no Reconciled call) with guidance to retry later or watch the
// orders stream.
func reconcileAsyncNotFound(ctx context.Context, cl orders.Client, e *ledger.Entry, out *render.ReconcileOutcomeView) {
	lctx, info := transport.WithCallInfo(ctx)
	list, err := cl.List(lctx, e.AccountID())
	if err != nil {
		out.Outcome = "indeterminate"
		out.Error = fmt.Sprintf("async order is not yet visible via GetOrderState and the order-list lookup failed; retry reconcile later: %s", render.Classify(err, callContext(info, false)).Message)
		return
	}
	for _, s := range list {
		if rid := s.GetOrderRequestId(); rid != "" && rid == e.OrderID() {
			setPlacedReconcileOutcome(out, s)
			_ = e.Reconciled(ledger.Result{OrderID: s.GetOrderId(), TrackingID: info.TrackingID()})
			return
		}
	}
	// Not confirmable yet: leave the intent unresolved for a later run.
	out.Outcome = "unresolved"
	out.Error = fmt.Sprintf("async order is not yet visible: PostOrderAsync orders become queryable only after exchange acceptance, with no fixed delay. Re-run `%s` later, or watch `tinvest stream orders --account %s`", reconcileCommand, e.AccountID())
}

func setPlacedReconcileOutcome(out *render.ReconcileOutcomeView, state *investapi.OrderState) {
	out.Outcome = "placed"
	out.OrderID = state.GetOrderId()
	out.Lifecycle = orders.Lifecycle(state.GetExecutionReportStatus())
}

func foreignIntentMessage(kind, command string) string {
	return fmt.Sprintf("intent kind %q belongs to another reconcile command; run `%s`", kind, command)
}

func reconcileTargetMismatch(entry *ledger.Entry, target reconcileTarget) (string, string) {
	if entry.Profile() != target.Profile {
		if entry.Profile() == "" {
			return "profile-mismatch", "intent was recorded without a named profile; rerun reconcile without --profile under its original endpoint"
		}
		return "profile-mismatch", fmt.Sprintf("intent belongs to profile %q; rerun with --profile %s", entry.Profile(), entry.Profile())
	}
	var payloadTarget struct {
		Endpoint string `json:"endpoint"`
	}
	if err := json.Unmarshal(entry.Payload(), &payloadTarget); err != nil {
		return "indeterminate", fmt.Sprintf("cannot determine the recorded endpoint from the journal payload: %v", err)
	}
	if payloadTarget.Endpoint == "" {
		return "indeterminate", "journal entry predates endpoint recording; it cannot be reconciled safely and must be checked manually"
	}
	if payloadTarget.Endpoint != target.Endpoint {
		return "profile-mismatch", fmt.Sprintf(
			"intent was recorded for endpoint %q, but the active profile uses %q; restore the recorded endpoint for profile %q and rerun reconcile",
			payloadTarget.Endpoint, target.Endpoint, target.Profile,
		)
	}
	return "", ""
}

func reconcileTable(w io.Writer, outcomes []render.ReconcileOutcomeView) error {
	rows := make([][]string, 0, len(outcomes)+1)
	foreignCount := 0
	for _, outcome := range outcomes {
		if outcome.Outcome == "foreign" {
			foreignCount++
		}
		rows = append(rows, []string{
			outcome.IntentID, outcome.ClientOrderID, outcome.Outcome,
			outcome.OrderID, outcome.Lifecycle, outcome.Error,
		})
	}
	if foreignCount > 0 {
		rows = append(rows, []string{
			"", "", "foreign-summary", "", "",
			fmt.Sprintf("%d foreign intent(s) skipped; use the command shown on each foreign line", foreignCount),
		})
	}
	return render.Table(w,
		[]string{"INTENT_ID", "CLIENT_ORDER_ID", "OUTCOME", "ORDER_ID", "LIFECYCLE", "DETAIL"},
		rows,
	)
}

// ---- shared helpers ----

// resolveOne resolves a single instrument id, mapping a malformed id to a usage
// error (exit 2) and a wire failure to the usual classification.
func (a *app) resolveOne(cmdCtx context.Context, conn *grpc.ClientConn, id string, noCache bool) (*investapi.Instrument, *render.CLIError, string) {
	insts, cerr, trackingID := a.resolveAll(cmdCtx, conn, []string{id}, noCache)
	if cerr != nil {
		return nil, cerr, trackingID
	}
	return insts[0], nil, trackingID
}

// openLedger opens the intent journal, honoring an override dir for tests.
func (a *app) openLedger() (*ledger.Ledger, error) {
	dir := a.ledgerDir
	if dir == "" {
		d, err := ledger.DefaultDir()
		if err != nil {
			return nil, err
		}
		dir = d
	}
	return ledger.Open(dir)
}

// newOrderID generates a random RFC 4122 v4 UUID using crypto/rand (no external
// dependency, per the M1 constraint).
func newOrderID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func validateOrderID(orderID string) error {
	if len(orderID) != 36 || orderID[8] != '-' || orderID[13] != '-' || orderID[18] != '-' || orderID[23] != '-' {
		return fmt.Errorf("order-id must be a UUID in canonical 8-4-4-4-12 format")
	}
	hexText := strings.ReplaceAll(orderID, "-", "")
	if len(hexText) != 32 {
		return fmt.Errorf("order-id must be a UUID in canonical 8-4-4-4-12 format")
	}
	if _, err := hex.DecodeString(hexText); err != nil {
		return fmt.Errorf("order-id must be a UUID in canonical 8-4-4-4-12 format")
	}
	return nil
}

func addCancelReconcileHint(cerr *render.CLIError, orderID, command string) *render.CLIError {
	if cerr != nil && cerr.Code == render.CodeUnconfirmed {
		cerr.ReconcileHint = &render.ReconcileHint{OrderID: orderID, Command: command}
	}
	return cerr
}

// placeTable renders a place/dry-run/replace result for humans.
func placeTable(w io.Writer, data placeData) error {
	switch {
	case data.DryRun:
		rows := [][]string{}
		if data.Preview != nil && data.Preview.TotalAmount != nil {
			rows = append(rows, []string{"total_order_amount", data.Preview.TotalAmount.Value})
		}
		if data.MaxLots != nil {
			rows = append(rows, []string{"buy_max_lots", fmt.Sprint(data.MaxLots.BuyMaxLots)})
			rows = append(rows, []string{"sell_max_lots", fmt.Sprint(data.MaxLots.SellMaxLots)})
		}
		return render.Table(w, []string{"FIELD", "VALUE"}, rows)
	case data.Preview != nil:
		rows := [][]string{{"lots_requested", fmt.Sprint(data.Preview.LotsRequested)}}
		for _, item := range []struct {
			name  string
			value *render.Decimal
		}{
			{"initial_order_amount", data.Preview.InitialAmount},
			{"total_order_amount", data.Preview.TotalAmount},
			{"executed_commission", data.Preview.Commission},
			{"executed_commission_rub", data.Preview.CommissionRub},
		} {
			if item.value != nil {
				rows = append(rows, []string{item.name, decimalView(item.value)})
			}
		}
		return render.Table(w, []string{"FIELD", "VALUE"}, rows)
	case data.MaxLots != nil:
		rows := [][]string{
			{"currency", data.MaxLots.Currency},
			{"buy_max_lots", fmt.Sprint(data.MaxLots.BuyMaxLots)},
			{"buy_max_market_lots", fmt.Sprint(data.MaxLots.BuyMaxMarketLot)},
			{"sell_max_lots", fmt.Sprint(data.MaxLots.SellMaxLots)},
		}
		return render.Table(w, []string{"FIELD", "VALUE"}, rows)
	case data.Async != nil:
		return render.Table(w, []string{"CLIENT_ORDER_ID", "TRADE_INTENT_ID", "LIFECYCLE"},
			[][]string{{data.Async.ClientOrderID, data.Async.TradeIntentID, data.Async.Lifecycle}})
	case data.Order != nil:
		o := data.Order
		return render.Table(w, []string{"ORDER_ID", "CLIENT_ORDER_ID", "LIFECYCLE", "REQUESTED", "EXECUTED"},
			[][]string{{o.OrderID, o.ClientOrderID, o.Lifecycle, fmt.Sprint(o.Lots.Requested), fmt.Sprint(o.Lots.Executed)}})
	}
	return nil
}

func decimalView(value *render.Decimal) string {
	if value == nil {
		return ""
	}
	if value.Currency == "" {
		return value.Value
	}
	return value.Value + " " + strings.ToUpper(value.Currency)
}
