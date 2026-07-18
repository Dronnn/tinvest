package retry

import (
	"context"
	"strings"
)

// readMethodPrefixes are RPC-name prefixes (the segment of the gRPC full
// method after the last "/") treated as reads by default. Deliberately just
// these two verbs — the contract's consistent naming for non-mutating calls
// (docs/plan-tinvest-cli.md §9) — so the prefix check can't silently start
// matching a mutation added to the contract later. Read RPCs that don't
// follow this convention go in readMethods below instead of widening this
// list.
var readMethodPrefixes = []string{"Get", "Find"}

// readMethods is the explicit allowlist of known read RPC full paths whose
// names don't start with Get/Find (confirmed against internal/pb/investapi,
// contract pinned per docs/plan-tinvest-cli.md §4). Every entry here is a
// side-effect-free lookup; nothing that creates, cancels, or mutates state.
var readMethods = map[string]bool{
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/BondBy":           true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Bonds":            true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Currencies":       true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/CurrencyBy":       true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/DfaBy":            true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Dfas":             true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/EtfBy":            true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Etfs":             true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/FutureBy":         true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures":          true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Indicatives":      true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/News":             true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/OptionBy":         true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Options":          true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/OptionsBy":        true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/ShareBy":          true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares":           true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/StructuredNoteBy": true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/StructuredNotes":  true,
	"/tinkoff.public.invest.api.contract.v1.InstrumentsService/TradingSchedules": true,
}

// isReadMethod reports whether fullMethod (e.g.
// "/tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts") is a
// recognized read RPC.
func isReadMethod(fullMethod string) bool {
	if readMethods[fullMethod] {
		return true
	}
	name := fullMethod
	if idx := strings.LastIndexByte(fullMethod, '/'); idx >= 0 {
		name = fullMethod[idx+1:]
	}
	for _, prefix := range readMethodPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// retryEligible reports whether a call may be retried at all: reads always
// are; mutations only when the caller opted in via Idempotent
// (docs/plan-tinvest-cli.md §9).
func retryEligible(ctx context.Context, fullMethod string) bool {
	return isReadMethod(fullMethod) || isIdempotent(ctx)
}
