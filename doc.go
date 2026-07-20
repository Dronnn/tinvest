// Package tinvest is a read-only Go client for the T-Bank Invest gRPC API,
// wrapping the same broker and transport layers the tinvest CLI uses over one
// shared connection with the full interceptor stack: Bearer auth, per-call
// deadlines, call-phase and x-tracking-id capture, the idempotency-aware retry
// policy, and client-side rate limiting.
//
// The *Client surface is deliberately read-only. Order placement, stop orders,
// sandbox mutations, streaming, and the intent ledger are intentionally absent
// and remain CLI-only by design; no *Client method mutates account state.
//
// This guarantee is about *Client only. The generated package
// github.com/Dronnn/tinvest/pb/investapi is the full T-Invest gRPC contract and
// necessarily exports the raw service clients, including mutating RPCs
// (PostOrder, CancelOrder, and the sandbox and stop-order services). Calling
// those directly bypasses this package's guardrails and is entirely at your own
// risk; the read-only guarantee does not extend to them.
//
// # Getting started
//
//	ctx := context.Background()
//	client, err := tinvest.New(ctx, tinvest.Config{Token: token})
//	if err != nil {
//		// handle error
//	}
//	defer client.Close()
//
//	inst, err := client.Resolve(ctx, "BBG004730N88") // uid, FIGI, or TICKER@CLASSCODE
//	if err != nil {
//		// handle error
//	}
//	prices, err := client.LastPrices(ctx, inst.GetUid())
//
// # Identifiers
//
// Every method that takes an instrument identifier accepts the same three
// shapes the CLI does — an instrument_uid, a FIGI, or a TICKER@CLASSCODE pair —
// and resolves it through the same resolver and local cache. A malformed
// identifier is reported as an error before any network call is made.
//
// # Types
//
// Methods return the generated protobuf types from
// github.com/Dronnn/tinvest/pb/investapi, or small result structs mirroring
// what the CLI prints. Money and Quotation values carry an exact units+nano
// pair; use QuotationString and MoneyString to render them as decimal strings
// without floating-point error.
package tinvest
