package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	brokerinstruments "tinvest/internal/broker/instruments"
	"tinvest/internal/broker/orders"
	"tinvest/internal/broker/stoporders"
	"tinvest/internal/ledger"
	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/policy"
	"tinvest/internal/render"
	"tinvest/internal/transport"
	"tinvest/internal/transport/retry"
)

// Ledger intent kind for stop-order placement (plan §9/§10).
const kindStopOrderPlace = "stoporder.place"

const stopReconcileCommand = "tinvest stop-orders reconcile"

func (a *app) stopOrdersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-orders",
		Short: "Place, list, cancel, and reconcile stop orders (take-profit, stop-loss, stop-limit)",
	}
	cmd.AddCommand(
		a.stopOrdersPlaceCmd(),
		a.stopOrdersListCmd(),
		a.stopOrdersCancelCmd(),
		a.stopOrdersReconcileCmd(),
	)
	return cmd
}

// ---- place ----

// stopPlaceFlags is the flag surface of `stop-orders place`, mirrored by
// stopPlaceInput for --input.
type stopPlaceFlags struct {
	instrument         string
	direction          string
	quantity           int64
	stopOrderType      string
	stopPrice          string
	price              string
	expiration         string
	expireDate         string
	exchangeOrderType  string
	takeProfitType     string
	trailingIndent     string
	trailingIndentType string
	trailingSpread     string
	trailingSpreadType string
	orderID            string
	dryRun             bool
	yes                bool
	input              string
	noCache            bool
}

func (a *app) stopOrdersPlaceCmd() *cobra.Command {
	var f stopPlaceFlags
	cmd := &cobra.Command{
		Use:   "place",
		Short: "Place a stop order (idempotent, journaled; never auto-retried — plan §9)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runPlaceStop(cmd, &f)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.instrument, "instrument", "", "instrument id: uid, FIGI, or TICKER@CLASSCODE")
	fl.StringVar(&f.direction, "direction", "", "buy or sell")
	fl.Int64Var(&f.quantity, "quantity", 0, "number of lots (positive)")
	fl.StringVar(&f.stopOrderType, "type", "", "take-profit, stop-loss, or stop-limit")
	fl.StringVar(&f.stopPrice, "stop-price", "", "stop-activation price as a decimal string (required)")
	fl.StringVar(&f.price, "price", "", "limit price as a decimal string (required for stop-limit only)")
	fl.StringVar(&f.expiration, "expiration", "gtc", "gtc or gtd")
	fl.StringVar(&f.expireDate, "expire-date", "", "RFC3339 timestamp; required when --expiration gtd")
	fl.StringVar(&f.exchangeOrderType, "exchange-order-type", "", "market or limit: the child order type for take-profit orders (default market)")
	fl.StringVar(&f.takeProfitType, "take-profit-type", "", "regular or trailing (take-profit only; default regular for take-profit)")
	fl.StringVar(&f.trailingIndent, "trailing-indent", "", "trailing take-profit indent, decimal string")
	fl.StringVar(&f.trailingIndentType, "trailing-indent-type", "", "absolute or relative")
	fl.StringVar(&f.trailingSpread, "trailing-spread", "", "trailing take-profit protective spread, decimal string")
	fl.StringVar(&f.trailingSpreadType, "trailing-spread-type", "", "absolute or relative")
	fl.StringVar(&f.orderID, "order-id", "", "client idempotency key (UUID); generated if omitted")
	fl.BoolVar(&f.dryRun, "dry-run", false, "validate only, no network call (stop orders have no GetOrderPrice/GetMaxLots equivalent)")
	fl.BoolVar(&f.yes, "yes", false, "confirm the mutation (accepted for symmetry; no interactive prompt)")
	fl.StringVar(&f.input, "input", "", "read the full request as JSON from a file or - for stdin")
	fl.BoolVar(&f.noCache, "no-cache", false, "bypass the local instrument cache")
	return cmd
}

// resolvedStopPlace is a stop-order place request after flag/JSON parsing and
// enum resolution, before any network call. The *Str fields keep the
// human-supplied form for --dry-run echo and journal payloads.
type resolvedStopPlace struct {
	instrument string

	direction    investapi.StopOrderDirection
	directionStr string

	stopOrderType investapi.StopOrderType
	typeStr       string

	quantity int64

	stopPrice    *investapi.Quotation
	stopPriceStr string

	price    *investapi.Quotation // stop-limit only
	priceStr string

	expirationType investapi.StopOrderExpirationType
	expirationStr  string

	expireDate    *timestamppb.Timestamp
	expireDateStr string

	exchangeOrderType    investapi.ExchangeOrderType
	exchangeOrderTypeStr string

	takeProfitType    investapi.TakeProfitType
	takeProfitTypeStr string

	trailing          *stoporders.TrailingParams
	trailingIndentStr string
	trailingSpreadStr string

	orderID string
	dryRun  bool
}

// runPlaceStop executes the stop-order placement flow (plan §9), mirroring
// runPlace's precedence with one deliberate difference: --dry-run is pure
// local validation with no network call at all (stop orders have no
// GetOrderPrice/GetMaxLots equivalent to preview against), and the placement
// send is never marked retry.Idempotent — the current contract's required
// order_id field on PostStopOrder has undocumented retention guarantees
// (plan §1.1/§9), so an ambiguous outcome always surfaces as exit 7 with a
// reconcile hint rather than being retried.
func (a *app) runPlaceStop(cmd *cobra.Command, f *stopPlaceFlags) error {
	start := time.Now()
	settings, cerr := a.settings()
	mode := render.Mode(settings.Output, os.Stdout)
	if cerr != nil {
		return a.fail(mode, cerr, render.NewMeta("", "", time.Since(start)))
	}
	metaNoNet := func() render.Meta { return render.NewMeta(settings.AccountID, "", time.Since(start)) }

	rp, cerr := resolveStopPlaceRequest(cmd, f)
	if cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}

	if err := stoporders.ValidateBasics(stopBasicsInput(rp)); err != nil {
		return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
	}

	if rp.dryRun {
		return a.stopDryRunResult(mode, rp, metaNoNet())
	}

	if cerr := requireAccount(settings); cerr != nil {
		return a.fail(mode, cerr, metaNoNet())
	}

	pol, err := policy.Load(settings.PolicyFile)
	if err != nil {
		return a.fail(mode, render.UsageError(err.Error()), metaNoNet())
	}
	// Reuse policy.OrderIntent/CheckLocal/CheckResolved (kill switch, allowlist,
	// lot cap, notional) rather than duplicating guardrail logic for stop
	// orders: OrderType is pinned to LIMIT so the market-order opt-in gate
	// (which governs immediate execution, not pending stop intents) never
	// fires, and Price carries stop_price so the notional cap is checked
	// against the stop-activation price, per plan §9's "notional vs stop_price".
	localIntent := policy.OrderIntent{
		Direction: stopToOrderDirection(rp.direction),
		OrderType: investapi.OrderType_ORDER_TYPE_LIMIT,
		Lots:      rp.quantity,
		Price:     rp.stopPrice,
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

	inst, cerr, trackingID := a.resolveOne(cmd.Context(), conn, rp.instrument, f.noCache)
	if cerr != nil {
		return a.fail(mode, cerr, render.NewMeta(settings.AccountID, trackingID, time.Since(start)))
	}
	uid := inst.GetUid()

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
	if err := orders.ValidatePriceIncrement(rp.stopPrice, inst.GetMinPriceIncrement()); err != nil {
		return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
	}
	if rp.price != nil {
		if err := orders.ValidatePriceIncrement(rp.price, inst.GetMinPriceIncrement()); err != nil {
			return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
		}
	}

	led, err := a.openLedger()
	if err != nil {
		return a.fail(mode, &render.CLIError{Code: render.CodeInternal, Message: err.Error()}, render.NewMeta(settings.AccountID, "", time.Since(start)))
	}
	defer func() { _ = led.Close() }()

	intent := ledger.Intent{
		IntentID:  rp.orderID,
		Kind:      kindStopOrderPlace,
		AccountID: settings.AccountID,
		Profile:   settings.Profile,
		Attempt:   1,
		OrderID:   rp.orderID,
		Payload:   stopOrderPayloadFrom(settings.AccountID, settings.Endpoint, uid, time.Now().UTC(), rp),
	}
	params := stoporders.PlaceParams{
		AccountID:         settings.AccountID,
		InstrumentID:      uid,
		OrderID:           rp.orderID,
		Direction:         rp.direction,
		StopOrderType:     rp.stopOrderType,
		Quantity:          rp.quantity,
		Price:             rp.price,
		StopPrice:         rp.stopPrice,
		ExpirationType:    rp.expirationType,
		ExpireDate:        rp.expireDate,
		ExchangeOrderType: rp.exchangeOrderType,
		TakeProfitType:    rp.takeProfitType,
		Trailing:          rp.trailing,
	}

	// Re-check the kill switch immediately before the send: the operator may
	// have engaged it during resolve / open-order lookups above (finding F11).
	if v := pol.CheckKillSwitch(); v != nil {
		return a.fail(mode, render.PolicyError(v.Message, v.Details), render.NewMeta(settings.AccountID, "", time.Since(start)))
	}

	resp, info, cerr := placeStopExec(cmd.Context(), stoporders.New(conn), led, intent, params)
	meta := render.NewMeta(settings.AccountID, infoTrackingID(info), time.Since(start))
	if cerr != nil {
		return a.fail(mode, cerr, meta)
	}
	view := render.PlaceStopResult(resp, rp.orderID)
	data := stopPlaceData{StopOrder: &view}
	if mode == "table" {
		return render.Table(os.Stdout, []string{"STOP_ORDER_ID", "CLIENT_ORDER_ID"}, [][]string{{view.StopOrderID, view.ClientOrderID}})
	}
	return render.WriteJSON(os.Stdout, render.Success(data, meta))
}

// stopPlaceData is the data block of a `stop-orders place` envelope.
type stopPlaceData struct {
	StopOrder *render.PlaceStopResultView `json:"stop_order,omitempty"`
	DryRun    bool                        `json:"dry_run,omitempty"`
	Would     *stopDryRunView             `json:"would_place,omitempty"`
}

// stopDryRunView echoes back a validated (but never sent) stop-order request.
type stopDryRunView struct {
	Instrument        string `json:"instrument"`
	Direction         string `json:"direction"`
	StopOrderType     string `json:"stop_order_type"`
	Quantity          int64  `json:"quantity"`
	StopPrice         string `json:"stop_price"`
	Price             string `json:"price,omitempty"`
	Expiration        string `json:"expiration"`
	ExpireDate        string `json:"expire_date,omitempty"`
	ExchangeOrderType string `json:"exchange_order_type,omitempty"`
	TakeProfitType    string `json:"take_profit_type,omitempty"`
}

// stopDryRunResult renders a --dry-run outcome: local validation passed, but
// nothing was sent and no ledger entry was created (plan §9 — stop orders
// have no GetOrderPrice/GetMaxLots equivalent, unlike `orders place --dry-run`).
func (a *app) stopDryRunResult(mode string, rp resolvedStopPlace, meta render.Meta) error {
	view := stopDryRunView{
		Instrument: rp.instrument, Direction: rp.directionStr, StopOrderType: rp.typeStr,
		Quantity: rp.quantity, StopPrice: rp.stopPriceStr, Price: rp.priceStr,
		Expiration: rp.expirationStr, ExpireDate: rp.expireDateStr,
		ExchangeOrderType: rp.exchangeOrderTypeStr, TakeProfitType: rp.takeProfitTypeStr,
	}
	data := stopPlaceData{DryRun: true, Would: &view}
	if mode == "table" {
		rows := [][]string{
			{"instrument", view.Instrument}, {"direction", view.Direction}, {"type", view.StopOrderType},
			{"quantity", fmt.Sprint(view.Quantity)}, {"stop_price", view.StopPrice}, {"price", view.Price},
		}
		return render.Table(os.Stdout, []string{"FIELD", "VALUE"}, rows)
	}
	return render.WriteJSON(os.Stdout, render.Success(data, meta))
}

// placeStopExec is the crash-safe stop-order placement sequence (plan §9),
// decoupled from cobra for testing. It journals Begin -> SendStarted before
// the send, then PostStopOrder -> Confirmed/Rejected. Unlike
// internal/broker/orders' placeExec, the send context is deliberately NEVER
// marked retry.Idempotent: the current contract's required order_id field on
// PostStopOrder has undocumented dedup retention (plan §1.1), so a repeated
// send on transient failure could duplicate the stop order rather than
// deduplicate. On an unconfirmable outcome (phase sent_unconfirmed) the
// ledger entry is deliberately left unresolved and the caller gets an exit-7
// error carrying the order_id and a reconcile hint pointing at
// `stop-orders reconcile` (which itself lists via `stop-orders list`).
func placeStopExec(cmdCtx context.Context, cl stoporders.Client, led *ledger.Ledger, intent ledger.Intent, p stoporders.PlaceParams) (*investapi.PostStopOrderResponse, *transport.CallInfo, *render.CLIError) {
	entry, err := led.Begin(intent)
	if err != nil {
		return nil, nil, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("ledger begin: %v", err)}
	}
	if err := entry.SendStarted(); err != nil {
		return nil, nil, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("ledger send-started: %v", err)}
	}

	// No retry.Idempotent(cmdCtx) here — see doc comment above. A plain ctx
	// means the transport's retry interceptor treats this as an ineligible
	// mutation and never retries it (internal/transport/retry: retryEligible
	// requires either a read method or an explicit Idempotent marker).
	ctx, info := transport.WithCallInfo(cmdCtx)
	resp, err := cl.Place(ctx, p)

	cc := callContext(info, true)
	if err != nil {
		cerr := render.Classify(err, cc)
		if cerr.Code == render.CodeUnconfirmed {
			cerr.ReconcileHint = &render.ReconcileHint{OrderID: p.OrderID, Command: stopReconcileCommand}
			return nil, info, cerr
		}
		_ = entry.Rejected(err)
		return nil, info, cerr
	}

	_ = entry.Confirmed(ledger.Result{StopOrderID: resp.GetStopOrderId(), TrackingID: info.TrackingID()})
	return resp, info, nil
}

func infoTrackingID(info *transport.CallInfo) string {
	if info == nil {
		return ""
	}
	return info.TrackingID()
}

func stopBasicsInput(rp resolvedStopPlace) stoporders.BasicsInput {
	return stoporders.BasicsInput{
		StopOrderType:     rp.stopOrderType,
		Quantity:          rp.quantity,
		Price:             rp.price,
		StopPrice:         rp.stopPrice,
		ExpirationType:    rp.expirationType,
		HasExpireDate:     rp.expireDate != nil,
		ExchangeOrderType: rp.exchangeOrderType,
		TakeProfitType:    rp.takeProfitType,
		Trailing:          rp.trailing,
	}
}

func stopToOrderDirection(d investapi.StopOrderDirection) investapi.OrderDirection {
	switch d {
	case investapi.StopOrderDirection_STOP_ORDER_DIRECTION_BUY:
		return investapi.OrderDirection_ORDER_DIRECTION_BUY
	case investapi.StopOrderDirection_STOP_ORDER_DIRECTION_SELL:
		return investapi.OrderDirection_ORDER_DIRECTION_SELL
	default:
		return investapi.OrderDirection_ORDER_DIRECTION_UNSPECIFIED
	}
}

// stopOrderPayload is the token-free request document journaled at Begin
// (plan §10), and the record reconcileStopFlow matches list results against.
type stopOrderPayload struct {
	AccountID          string `json:"account_id"`
	Endpoint           string `json:"endpoint"`
	InstrumentID       string `json:"instrument_id"`
	OrderID            string `json:"order_id"`
	Direction          string `json:"direction"`
	StopOrderType      string `json:"stop_order_type"`
	Quantity           int64  `json:"quantity"`
	Price              string `json:"price,omitempty"`
	StopPrice          string `json:"stop_price"`
	ExpirationType     string `json:"expiration_type"`
	ExpireDate         string `json:"expire_date,omitempty"`
	ExchangeOrderType  string `json:"exchange_order_type,omitempty"`
	TakeProfitType     string `json:"take_profit_type,omitempty"`
	TrailingIndent     string `json:"trailing_indent,omitempty"`
	TrailingIndentType string `json:"trailing_indent_type,omitempty"`
	TrailingSpread     string `json:"trailing_spread,omitempty"`
	TrailingSpreadType string `json:"trailing_spread_type,omitempty"`
	CreatedAt          string `json:"created_at"`
}

func stopOrderPayloadFrom(accountID, endpoint, uid string, createdAt time.Time, rp resolvedStopPlace) stopOrderPayload {
	exchangeOrderType := ""
	takeProfitType := ""
	trailingIndentType := ""
	trailingSpreadType := ""
	if rp.stopOrderType == investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT {
		exchangeOrderType = rp.exchangeOrderType.String()
		takeProfitType = rp.takeProfitType.String()
		if rp.trailing != nil {
			trailingIndentType = rp.trailing.IndentType.String()
			trailingSpreadType = rp.trailing.SpreadType.String()
		}
	}
	return stopOrderPayload{
		AccountID:          accountID,
		Endpoint:           endpoint,
		InstrumentID:       uid,
		OrderID:            rp.orderID,
		Direction:          rp.direction.String(),
		StopOrderType:      rp.stopOrderType.String(),
		Quantity:           rp.quantity,
		Price:              rp.priceStr,
		StopPrice:          rp.stopPriceStr,
		ExpirationType:     rp.expirationType.String(),
		ExpireDate:         rp.expireDateStr,
		ExchangeOrderType:  exchangeOrderType,
		TakeProfitType:     takeProfitType,
		TrailingIndent:     rp.trailingIndentStr,
		TrailingIndentType: trailingIndentType,
		TrailingSpread:     rp.trailingSpreadStr,
		TrailingSpreadType: trailingSpreadType,
		CreatedAt:          createdAt.Format(time.RFC3339Nano),
	}
}

// ---- JSON input (--input) ----

// stopPlaceInput is the JSON document accepted by `stop-orders place --input`.
// It mirrors the flag surface exactly. Schema:
//
//	{
//	  "instrument":           "<uid | FIGI | TICKER@CLASSCODE>",  // required
//	  "direction":            "buy" | "sell",                      // required
//	  "quantity":             <int lots > 0>,                      // required
//	  "type":                 "take-profit" | "stop-loss" | "stop-limit", // required
//	  "stop_price":           "<decimal string>",                  // required
//	  "price":                "<decimal string>",                  // required for stop-limit only
//	  "expiration":            "gtc" | "gtd",                       // optional, default gtc
//	  "expire_date":           "<RFC3339>",                         // required for gtd
//	  "exchange_order_type":   "market" | "limit",                  // optional, default market
//	  "take_profit_type":      "regular" | "trailing",               // optional, default regular
//	  "trailing_indent":       "<decimal string>",
//	  "trailing_indent_type":  "absolute" | "relative",
//	  "trailing_spread":       "<decimal string>",
//	  "trailing_spread_type":  "absolute" | "relative",
//	  "order_id":              "<uuid>",                            // optional; generated
//	  "dry_run":                <bool>                              // optional
//	}
//
// Unknown fields are rejected so a misspelled key fails loudly rather than
// silently dropping a safety-relevant value.
type stopPlaceInput struct {
	Instrument         string `json:"instrument"`
	Direction          string `json:"direction"`
	Quantity           int64  `json:"quantity"`
	Type               string `json:"type"`
	StopPrice          string `json:"stop_price"`
	Price              string `json:"price,omitempty"`
	Expiration         string `json:"expiration,omitempty"`
	ExpireDate         string `json:"expire_date,omitempty"`
	ExchangeOrderType  string `json:"exchange_order_type,omitempty"`
	TakeProfitType     string `json:"take_profit_type,omitempty"`
	TrailingIndent     string `json:"trailing_indent,omitempty"`
	TrailingIndentType string `json:"trailing_indent_type,omitempty"`
	TrailingSpread     string `json:"trailing_spread,omitempty"`
	TrailingSpreadType string `json:"trailing_spread_type,omitempty"`
	OrderID            string `json:"order_id,omitempty"`
	DryRun             bool   `json:"dry_run,omitempty"`
}

// resolveStopPlaceRequest turns flags or JSON input into a resolvedStopPlace,
// enforcing that --input and the order-shaping flags are mutually exclusive
// (plan §7).
func resolveStopPlaceRequest(cmd *cobra.Command, f *stopPlaceFlags) (resolvedStopPlace, *render.CLIError) {
	fl := cmd.Flags()
	if f.input != "" {
		names := []string{
			"instrument", "direction", "quantity", "type", "stop-price", "price",
			"expiration", "expire-date", "exchange-order-type", "take-profit-type",
			"trailing-indent", "trailing-indent-type", "trailing-spread", "trailing-spread-type",
			"order-id",
		}
		for _, name := range names {
			if fl.Changed(name) {
				return resolvedStopPlace{}, render.UsageError("--input is mutually exclusive with order flags (e.g. --" + name + ")")
			}
		}
		return resolveStopPlaceInput(f.input)
	}
	return buildStopPlace(stopPlaceInput{
		Instrument: f.instrument, Direction: f.direction, Quantity: f.quantity, Type: f.stopOrderType,
		StopPrice: f.stopPrice, Price: f.price, Expiration: f.expiration, ExpireDate: f.expireDate,
		ExchangeOrderType: f.exchangeOrderType, TakeProfitType: f.takeProfitType,
		TrailingIndent: f.trailingIndent, TrailingIndentType: f.trailingIndentType,
		TrailingSpread: f.trailingSpread, TrailingSpreadType: f.trailingSpreadType,
		OrderID: f.orderID, DryRun: f.dryRun,
	})
}

func resolveStopPlaceInput(source string) (resolvedStopPlace, *render.CLIError) {
	var reader io.Reader
	if source == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(source)
		if err != nil {
			return resolvedStopPlace{}, render.UsageError(fmt.Sprintf("open input %s: %v", source, err))
		}
		defer func() { _ = file.Close() }()
		reader = file
	}
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	var in stopPlaceInput
	if err := dec.Decode(&in); err != nil {
		return resolvedStopPlace{}, render.UsageError(fmt.Sprintf("invalid JSON input: %v", err))
	}
	return buildStopPlace(in)
}

// buildStopPlace validates and resolves a stopPlaceInput (from flags or
// JSON) into a resolvedStopPlace: it classifies the instrument id, parses the
// enums, prices, and expire-date, and generates a client order_id when none
// was supplied (plan §9 — persisted to the ledger before the send).
func buildStopPlace(in stopPlaceInput) (resolvedStopPlace, *render.CLIError) {
	if _, err := brokerinstruments.Classify(in.Instrument); err != nil {
		return resolvedStopPlace{}, render.UsageError(err.Error())
	}
	direction, err := stoporders.Direction(in.Direction)
	if err != nil {
		return resolvedStopPlace{}, render.UsageError(err.Error())
	}
	stopOrderType, err := stoporders.Type(in.Type)
	if err != nil {
		return resolvedStopPlace{}, render.UsageError(err.Error())
	}
	expirationType, err := stoporders.Expiration(in.Expiration)
	if err != nil {
		return resolvedStopPlace{}, render.UsageError(err.Error())
	}
	exchangeOrderType := investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_UNSPECIFIED
	exchangeOrderTypeStr := ""
	if stopOrderType == investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT {
		exchangeOrderType, err = stoporders.ExchangeOrderType(in.ExchangeOrderType)
		if err != nil {
			return resolvedStopPlace{}, render.UsageError(err.Error())
		}
		exchangeOrderTypeStr = defaultStr(in.ExchangeOrderType, "market")
	} else if strings.TrimSpace(in.ExchangeOrderType) != "" {
		return resolvedStopPlace{}, render.UsageError("--exchange-order-type is only valid with --type take-profit")
	}
	takeProfitType := investapi.TakeProfitType_TAKE_PROFIT_TYPE_UNSPECIFIED
	takeProfitTypeStr := ""
	if stopOrderType == investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT {
		takeProfitType, err = stoporders.TakeProfitType(in.TakeProfitType)
		if err != nil {
			return resolvedStopPlace{}, render.UsageError(err.Error())
		}
		takeProfitTypeStr = defaultStr(in.TakeProfitType, "regular")
	} else if strings.TrimSpace(in.TakeProfitType) != "" {
		return resolvedStopPlace{}, render.UsageError("--take-profit-type is only valid with --type take-profit")
	}

	stopPriceStr := strings.TrimSpace(in.StopPrice)
	stopPrice, perr := parseRequiredDecimal("stop-price", stopPriceStr)
	if perr != nil {
		return resolvedStopPlace{}, render.UsageError(perr.Error())
	}

	var price *investapi.Quotation
	priceStr := strings.TrimSpace(in.Price)
	if priceStr != "" {
		q, err := render.ParseQuotation(priceStr)
		if err != nil {
			return resolvedStopPlace{}, render.UsageError(fmt.Sprintf("invalid --price %q: %v", in.Price, err))
		}
		price = q
	}

	var expireDate *timestamppb.Timestamp
	expireDateStr := strings.TrimSpace(in.ExpireDate)
	if expireDateStr != "" {
		t, err := time.Parse(time.RFC3339, expireDateStr)
		if err != nil {
			return resolvedStopPlace{}, render.UsageError(fmt.Sprintf("invalid --expire-date %q: want RFC3339 (e.g. 2026-08-01T00:00:00Z): %v", in.ExpireDate, err))
		}
		expireDate = timestamppb.New(t.UTC())
	}

	trailing, indentStr, spreadStr, cerr := buildTrailing(in)
	if cerr != nil {
		return resolvedStopPlace{}, cerr
	}

	orderID := strings.TrimSpace(in.OrderID)
	if orderID == "" {
		generated, err := newOrderID()
		if err != nil {
			return resolvedStopPlace{}, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("generate order id: %v", err)}
		}
		orderID = generated
	}
	if err := validateOrderID(orderID); err != nil {
		return resolvedStopPlace{}, render.UsageError(err.Error())
	}

	return resolvedStopPlace{
		instrument:           in.Instrument,
		direction:            direction,
		directionStr:         in.Direction,
		stopOrderType:        stopOrderType,
		typeStr:              in.Type,
		quantity:             in.Quantity,
		stopPrice:            stopPrice,
		stopPriceStr:         stopPriceStr,
		price:                price,
		priceStr:             priceStr,
		expirationType:       expirationType,
		expirationStr:        defaultStr(in.Expiration, "gtc"),
		expireDate:           expireDate,
		expireDateStr:        expireDateStr,
		exchangeOrderType:    exchangeOrderType,
		exchangeOrderTypeStr: exchangeOrderTypeStr,
		takeProfitType:       takeProfitType,
		takeProfitTypeStr:    takeProfitTypeStr,
		trailing:             trailing,
		trailingIndentStr:    indentStr,
		trailingSpreadStr:    spreadStr,
		orderID:              orderID,
		dryRun:               in.DryRun,
	}, nil
}

func buildTrailing(in stopPlaceInput) (*stoporders.TrailingParams, string, string, *render.CLIError) {
	indentStr := strings.TrimSpace(in.TrailingIndent)
	spreadStr := strings.TrimSpace(in.TrailingSpread)
	if indentStr == "" && spreadStr == "" && in.TrailingIndentType == "" && in.TrailingSpreadType == "" {
		return nil, "", "", nil
	}
	if indentStr == "" || spreadStr == "" || in.TrailingIndentType == "" || in.TrailingSpreadType == "" {
		return nil, "", "", render.UsageError("trailing requires all of --trailing-indent, --trailing-indent-type, --trailing-spread, and --trailing-spread-type")
	}
	indent, err := render.ParseQuotation(indentStr)
	if err != nil {
		return nil, "", "", render.UsageError(fmt.Sprintf("invalid --trailing-indent %q: %v", indentStr, err))
	}
	spread, err := render.ParseQuotation(spreadStr)
	if err != nil {
		return nil, "", "", render.UsageError(fmt.Sprintf("invalid --trailing-spread %q: %v", spreadStr, err))
	}
	indentType, err := stoporders.TrailingValueType(in.TrailingIndentType)
	if err != nil {
		return nil, "", "", render.UsageError(err.Error())
	}
	spreadType, err := stoporders.TrailingValueType(in.TrailingSpreadType)
	if err != nil {
		return nil, "", "", render.UsageError(err.Error())
	}
	return &stoporders.TrailingParams{Indent: indent, IndentType: indentType, Spread: spread, SpreadType: spreadType}, indentStr, spreadStr, nil
}

func parseRequiredDecimal(field, s string) (*investapi.Quotation, error) {
	if s == "" {
		return nil, fmt.Errorf("--%s is required", field)
	}
	q, err := render.ParseQuotation(s)
	if err != nil {
		return nil, fmt.Errorf("invalid --%s %q: %w", field, s, err)
	}
	return q, nil
}

func defaultStr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// ---- list ----

type stopOrdersListData struct {
	StopOrders []render.StopOrderView `json:"stop_orders"`
}

func (a *app) stopOrdersListCmd() *cobra.Command {
	var status string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List stop orders on the account",
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
			st, err := stoporders.Status(status)
			if err != nil {
				return a.fail(mode, render.UsageError(err.Error()), render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			conn, cerr := a.connect(cmd.Context(), settings)
			if cerr != nil {
				return a.fail(mode, cerr, render.NewMeta(settings.AccountID, "", time.Since(start)))
			}
			defer func() { _ = conn.Close() }()

			ctx, info := transport.WithCallInfo(cmd.Context())
			list, err := stoporders.New(conn).List(ctx, stoporders.ListParams{AccountID: settings.AccountID, Status: st})
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				return a.fail(mode, render.Classify(err, callContext(info, false)), meta)
			}
			views := make([]render.StopOrderView, 0, len(list))
			for _, s := range list {
				views = append(views, render.StopOrder(s, stoporders.StatusName(s.GetStatus())))
			}
			if mode == "table" {
				return render.StopOrdersTable(os.Stdout, views)
			}
			return render.WriteJSON(os.Stdout, render.Success(stopOrdersListData{StopOrders: views}, meta))
		},
	}
	cmd.Flags().StringVar(&status, "status", "", "filter: all, active, executed, canceled, or expired (default: broker's own default)")
	return cmd
}

// ---- cancel ----

func (a *app) stopOrdersCancelCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel <stop-order-id>",
		Short: "Cancel an active stop order (idempotent)",
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
			// CancelStopOrder is convergent when repeated (see
			// stoporders.Client.Cancel's doc comment) — unlike Place, retry here
			// is safe.
			ctx, info := transport.WithCallInfo(retry.Idempotent(cmd.Context()))
			resp, err := stoporders.New(conn).Cancel(ctx, settings.AccountID, args[0])
			meta := render.NewMeta(settings.AccountID, info.TrackingID(), time.Since(start))
			if err != nil {
				cerr := render.Classify(err, callContext(info, true))
				return a.fail(mode, addCancelReconcileHint(cerr, args[0], "tinvest stop-orders list --status all"), meta)
			}
			data := cancelData{OrderID: args[0], Time: render.Timestamp(resp.GetTime())}
			if mode == "table" {
				return render.Table(os.Stdout, []string{"STOP_ORDER_ID", "CANCELLED_AT"}, [][]string{{data.OrderID, data.Time}})
			}
			return render.WriteJSON(os.Stdout, render.Success(data, meta))
		},
	}
	return cmd
}

// ---- reconcile ----

func (a *app) stopOrdersReconcileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile",
		Short: "Resolve every unconfirmed stop-order intent in the journal against the broker",
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

			outcomes, cerr := reconcileStopFlowForTarget(
				cmd.Context(), stoporders.New(conn), led,
				reconcileTarget{Profile: settings.Profile, Endpoint: settings.Endpoint},
			)
			meta := render.NewMeta(settings.AccountID, "", time.Since(start))
			if cerr != nil {
				return a.fail(mode, cerr, meta)
			}
			if mode == "table" {
				return reconcileTable(os.Stdout, outcomes)
			}
			return render.WriteJSON(os.Stdout, render.Success(newReconcileData(outcomes, reconcileCommand), meta))
		},
	}
}

// reconcileStopFlowForTarget lists all stop-order statuses and matches every
// available request field, including broker creation time. Sent intents with
// no exact match remain unresolved because the broker may have accepted and
// already removed the stop order from retained list history.
func reconcileStopFlowForTarget(
	ctx context.Context,
	cl stoporders.Client,
	led *ledger.Ledger,
	target reconcileTarget,
) ([]render.ReconcileOutcomeView, *render.CLIError) {
	entries, err := led.Unresolved()
	if err != nil {
		return nil, &render.CLIError{Code: render.CodeInternal, Message: fmt.Sprintf("read journal: %v", err)}
	}

	outcomes := make([]render.ReconcileOutcomeView, 0, len(entries))
	listByAccount := map[string][]*investapi.StopOrder{}

	for _, e := range entries {
		out := render.ReconcileOutcomeView{IntentID: e.IntentID(), ClientOrderID: e.OrderID(), AccountID: e.AccountID()}
		if e.Kind() != kindStopOrderPlace {
			out.Outcome = "foreign"
			out.Error = foreignIntentMessage(e.Kind(), reconcileCommand)
			outcomes = append(outcomes, out)
			continue
		}
		if outcome, message := reconcileTargetMismatch(e, target); message != "" {
			out.Outcome = outcome
			out.Error = message
			outcomes = append(outcomes, out)
			continue
		}
		if e.AccountID() == "" || e.OrderID() == "" {
			out.Outcome = "indeterminate"
			out.Error = "missing account or order id in journal entry"
			outcomes = append(outcomes, out)
			continue
		}

		var payload stopOrderPayload
		if err := json.Unmarshal(e.Payload(), &payload); err != nil {
			out.Outcome = "indeterminate"
			out.Error = fmt.Sprintf("unreadable journal payload: %v", err)
			outcomes = append(outcomes, out)
			continue
		}
		if err := validateStopMatchPayload(payload); err != nil {
			out.Outcome = "indeterminate"
			out.Error = fmt.Sprintf("journal payload cannot support safe stop-order matching: %v", err)
			outcomes = append(outcomes, out)
			continue
		}

		list, ok := listByAccount[e.AccountID()]
		if !ok {
			cctx, info := transport.WithCallInfo(ctx)
			l, err := cl.List(cctx, stoporders.ListParams{
				AccountID: e.AccountID(),
				Status:    investapi.StopOrderStatusOption_STOP_ORDER_STATUS_ALL,
			})
			if err != nil {
				out.Outcome = "error"
				out.Error = render.Classify(err, callContext(info, false)).Message
				outcomes = append(outcomes, out)
				continue
			}
			list = l
			listByAccount[e.AccountID()] = list
		}

		matches := matchStopOrders(list, payload)
		switch len(matches) {
		case 0:
			if e.Stage() == ledger.StageIntentCreated {
				out.Outcome = "not-placed"
				_ = e.Reconciled(ledger.Result{Error: "not-placed-before-send"})
			} else {
				out.Outcome = "indeterminate"
				out.Error = "no exact stop-order match was found in status=ALL; the broker may have accepted and then executed, canceled, or expired it; check manually with `tinvest stop-orders list --status all` and retry reconcile later"
			}
		case 1:
			out.Outcome = "placed"
			out.OrderID = matches[0].GetStopOrderId()
			out.Lifecycle = stoporders.StatusName(matches[0].GetStatus())
			_ = e.Reconciled(ledger.Result{StopOrderID: matches[0].GetStopOrderId()})
		default:
			out.Outcome = "ambiguous"
			out.Error = fmt.Sprintf("%d candidate stop orders match this intent; resolve manually with `tinvest stop-orders list --status all`", len(matches))
		}
		outcomes = append(outcomes, out)
	}
	return outcomes, nil
}

const (
	stopReconcileClockSkew      = 5 * time.Second
	stopReconcileCreationWindow = 2 * time.Minute
)

// matchStopOrders finds every listed stop order consistent with all request
// fields echoed by GetStopOrders, plus a bounded creation window around the
// journaled intent time.
func matchStopOrders(list []*investapi.StopOrder, p stopOrderPayload) []*investapi.StopOrder {
	var out []*investapi.StopOrder
	for _, s := range list {
		if s.GetInstrumentUid() != p.InstrumentID {
			continue
		}
		if s.GetDirection().String() != p.Direction {
			continue
		}
		if s.GetOrderType().String() != p.StopOrderType {
			continue
		}
		if s.GetLotsRequested() != p.Quantity {
			continue
		}
		if !moneyEqualsDecimal(s.GetStopPrice(), p.StopPrice) {
			continue
		}
		if !moneyEqualsDecimal(s.GetPrice(), p.Price) {
			continue
		}
		if !stopExpirationMatches(s, p) {
			continue
		}
		if !stopTakeProfitMatches(s, p) {
			continue
		}
		if !stopCreationMatches(s, p.CreatedAt) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func validateStopMatchPayload(p stopOrderPayload) error {
	if p.InstrumentID == "" || p.Direction == "" || p.StopOrderType == "" || p.Quantity <= 0 ||
		p.StopPrice == "" || p.ExpirationType == "" || p.CreatedAt == "" {
		return fmt.Errorf("one or more full-match fields are missing")
	}
	if _, err := time.Parse(time.RFC3339Nano, p.CreatedAt); err != nil {
		return fmt.Errorf("invalid created_at: %w", err)
	}
	if p.ExpirationType == investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE.String() {
		if _, err := time.Parse(time.RFC3339, p.ExpireDate); err != nil {
			return fmt.Errorf("invalid expire_date: %w", err)
		}
	}
	if p.StopOrderType == investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT.String() {
		if p.ExchangeOrderType == "" || p.TakeProfitType == "" {
			return fmt.Errorf("one or more take-profit match fields are missing")
		}
	} else if p.ExchangeOrderType != "" || p.TakeProfitType != "" {
		return fmt.Errorf("take-profit-only match fields are present for another stop type")
	}
	if p.TakeProfitType == investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING.String() &&
		(p.TrailingIndent == "" || p.TrailingIndentType == "" || p.TrailingSpread == "" || p.TrailingSpreadType == "") {
		return fmt.Errorf("trailing take-profit fields are missing")
	}
	return nil
}

func stopExpirationMatches(stop *investapi.StopOrder, payload stopOrderPayload) bool {
	switch payload.ExpirationType {
	case investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL.String():
		return payload.ExpireDate == "" && stop.GetExpirationTime() == nil
	case investapi.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE.String():
		expected, err := time.Parse(time.RFC3339, payload.ExpireDate)
		return err == nil && stop.GetExpirationTime() != nil && stop.GetExpirationTime().AsTime().Equal(expected)
	default:
		return false
	}
}

func stopTakeProfitMatches(stop *investapi.StopOrder, payload stopOrderPayload) bool {
	if payload.StopOrderType != investapi.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT.String() {
		return payload.ExchangeOrderType == "" && payload.TakeProfitType == "" &&
			stop.GetExchangeOrderType() == investapi.ExchangeOrderType_EXCHANGE_ORDER_TYPE_UNSPECIFIED &&
			stop.GetTakeProfitType() == investapi.TakeProfitType_TAKE_PROFIT_TYPE_UNSPECIFIED &&
			stop.GetTrailingData() == nil
	}
	if stop.GetExchangeOrderType().String() != payload.ExchangeOrderType {
		return false
	}
	if stop.GetTakeProfitType().String() != payload.TakeProfitType {
		return false
	}
	if payload.TakeProfitType != investapi.TakeProfitType_TAKE_PROFIT_TYPE_TRAILING.String() {
		return stop.GetTrailingData() == nil
	}
	trailing := stop.GetTrailingData()
	return trailing != nil &&
		quotationEqualsDecimal(trailing.GetIndent(), payload.TrailingIndent) &&
		trailing.GetIndentType().String() == payload.TrailingIndentType &&
		quotationEqualsDecimal(trailing.GetSpread(), payload.TrailingSpread) &&
		trailing.GetSpreadType().String() == payload.TrailingSpreadType
}

func stopCreationMatches(stop *investapi.StopOrder, createdAt string) bool {
	intentTime, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil || stop.GetCreateDate() == nil {
		return false
	}
	created := stop.GetCreateDate().AsTime()
	return !created.Before(intentTime.Add(-stopReconcileClockSkew)) &&
		!created.After(intentTime.Add(stopReconcileCreationWindow))
}

func moneyEqualsDecimal(m *investapi.MoneyValue, decimal string) bool {
	if decimal == "" {
		return m == nil
	}
	q, err := render.ParseQuotation(decimal)
	if err != nil {
		return false
	}
	return m.GetUnits() == q.GetUnits() && m.GetNano() == q.GetNano()
}

func quotationEqualsDecimal(q *investapi.Quotation, decimal string) bool {
	if decimal == "" {
		return q == nil
	}
	expected, err := render.ParseQuotation(decimal)
	if err != nil || q == nil {
		return false
	}
	return q.GetUnits() == expected.GetUnits() && q.GetNano() == expected.GetNano()
}
