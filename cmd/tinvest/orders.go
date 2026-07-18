package main

import (
	"context"
	"crypto/rand"
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
	AccountID    string `json:"account_id"`
	InstrumentID string `json:"instrument_id"`
	OrderID      string `json:"order_id"`
	Direction    string `json:"direction"`
	OrderType    string `json:"order_type"`
	Lots         int64  `json:"lots"`
	Price        string `json:"price,omitempty"`
	TimeInForce  string `json:"time_in_force,omitempty"`
	Async        bool   `json:"async,omitempty"`
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
	instrument string
	direction  string
	quantity   int64
	orderType  string
	price      string
	tif        string
	orderID    string
	async      bool
	dryRun     bool
	yes        bool
	input      string
	noCache    bool
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
	fl.BoolVar(&f.dryRun, "dry-run", false, "validate and preview only; place nothing")
	fl.BoolVar(&f.yes, "yes", false, "confirm the mutation (accepted for symmetry; no interactive prompt)")
	fl.StringVar(&f.input, "input", "", "read the full request as JSON from a file or - for stdin")
	fl.BoolVar(&f.noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

// resolvedPlace is a place request after flag/JSON parsing and enum resolution,
// before any network call. Price is nil for market/bestprice.
type resolvedPlace struct {
	instrument string
	direction  investapi.OrderDirection
	orderType  investapi.OrderType
	lots       int64
	price      *investapi.Quotation
	priceStr   string
	tif        investapi.TimeInForceType
	tifStr     string
	orderID    string
	async      bool
	dryRun     bool
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
		Price:     rp.price,
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
			AccountID:    settings.AccountID,
			InstrumentID: uid,
			OrderID:      rp.orderID,
			Direction:    rp.direction.String(),
			OrderType:    rp.orderType.String(),
			Lots:         rp.lots,
			Price:        rp.priceStr,
			TimeInForce:  rp.tifStr,
			Async:        rp.async,
		},
	}
	params := orders.PlaceParams{
		AccountID:    settings.AccountID,
		InstrumentID: uid,
		OrderID:      rp.orderID,
		Direction:    rp.direction,
		OrderType:    rp.orderType,
		Lots:         rp.lots,
		Price:        rp.price,
		TimeInForce:  rp.tif,
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
		for _, name := range []string{"instrument", "direction", "quantity", "type", "price", "tif", "order-id", "async"} {
			if fl.Changed(name) {
				return resolvedPlace{}, render.UsageError("--input is mutually exclusive with order flags (e.g. --" + name + ")")
			}
		}
		return resolvePlaceInput(f.input)
	}
	return buildPlace(placeInput{
		Instrument: f.instrument,
		Direction:  f.direction,
		Quantity:   f.quantity,
		Type:       f.orderType,
		Price:      f.price,
		TIF:        f.tif,
		OrderID:    f.orderID,
		Async:      f.async,
		DryRun:     f.dryRun,
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
//	  "order_id":   "<uuid, <=36 chars>",                // optional; generated
//	  "async":      <bool>,                              // optional
//	  "dry_run":    <bool>                               // optional
//	}
//
// Unknown fields are rejected so a misspelled key fails loudly rather than
// silently dropping a safety-relevant value.
type placeInput struct {
	Instrument string `json:"instrument"`
	Direction  string `json:"direction"`
	Quantity   int64  `json:"quantity"`
	Type       string `json:"type"`
	Price      string `json:"price,omitempty"`
	TIF        string `json:"tif,omitempty"`
	OrderID    string `json:"order_id,omitempty"`
	Async      bool   `json:"async,omitempty"`
	DryRun     bool   `json:"dry_run,omitempty"`
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
	if len(orderID) > 36 {
		return resolvedPlace{}, render.UsageError("order-id must be at most 36 characters")
	}

	return resolvedPlace{
		instrument: in.Instrument,
		direction:  direction,
		orderType:  orderType,
		lots:       in.Quantity,
		price:      price,
		priceStr:   priceStr,
		tif:        tif,
		tifStr:     in.TIF,
		orderID:    orderID,
		async:      in.Async,
		dryRun:     in.DryRun,
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

			// CancelOrder is convergent when repeated, so retry is safe.
			ctx, info := transport.WithCallInfo(retry.Idempotent(cmd.Context()))
			resp, err := orders.New(conn).Cancel(ctx, settings.AccountID, args[0])
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, true)), meta)
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
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

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

			entry, err := led.Begin(ledger.Intent{
				IntentID: key, Kind: kindOrderReplace, AccountID: settings.AccountID,
				Profile: settings.Profile, Attempt: 1, OrderID: key,
				Payload: map[string]any{"replaces": args[0], "quantity": quantity, "price": price},
			})
			if err != nil {
				return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			_ = entry.SendStarted()

			ctx, info := transport.WithCallInfo(cmd.Context())
			resp, err := orders.New(conn).Replace(ctx, orders.ReplaceParams{
				AccountID: settings.AccountID, OrderID: args[0], IdempotencyKey: key,
				Lots: quantity, Price: priceQ,
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
	return cmd
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
			return render.WriteJSON(os.Stdout, render.Success(maxLotsData{MaxLots: render.MaxLots(resp)}, meta))
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
	Outcomes []render.ReconcileOutcomeView `json:"outcomes"`
}

func (a *app) ordersReconcileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile",
		Short: "Resolve every unconfirmed intent in the journal against the broker",
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

			outcomes, cerr := reconcileFlow(cmd.Context(), orders.New(conn), led)
			meta := render.NewMeta(settings.AccountID, "", time.Since(start))
			if cerr != nil {
				return a.fail(mode, cerr, meta)
			}
			if mode == "table" {
				return render.ReconcileTable(os.Stdout, outcomes)
			}
			return render.WriteJSON(os.Stdout, render.Success(reconcileData{Outcomes: outcomes}, meta))
		},
	}
}

// reconcileFlow resolves every Unresolved ledger entry against the broker
// (plan §9). For each intent it queries GetOrderState by the client
// idempotency key: a found order is closed out as Reconciled with its
// lifecycle; a NOT_FOUND means the order never reached the broker, closed out
// as "not-placed"; a transient error leaves the entry unresolved for the next
// run. Reconcile is decoupled from cobra for testing.
func reconcileFlow(ctx context.Context, cl orders.Client, led *ledger.Ledger) ([]render.ReconcileOutcomeView, *render.CLIError) {
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
		if e.OrderID() == "" || e.AccountID() == "" {
			// Nothing to look the order up by; leave it for a human.
			out.Outcome = "indeterminate"
			out.Error = "missing account or order id in journal entry"
			outcomes = append(outcomes, out)
			continue
		}
		cctx, info := transport.WithCallInfo(ctx)
		state, err := cl.Get(cctx, e.AccountID(), e.OrderID(), true)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				out.Outcome = "not-placed"
				_ = e.Reconciled(ledger.Result{Error: "not-placed"})
			} else {
				out.Outcome = "error"
				out.Error = render.Classify(err, callContext(info, false)).Message
			}
			outcomes = append(outcomes, out)
			continue
		}
		lifecycle := orders.Lifecycle(state.GetExecutionReportStatus())
		out.Outcome = "placed"
		out.OrderID = state.GetOrderId()
		out.Lifecycle = lifecycle
		_ = e.Reconciled(ledger.Result{OrderID: state.GetOrderId(), TrackingID: info.TrackingID()})
		outcomes = append(outcomes, out)
	}
	return outcomes, nil
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
