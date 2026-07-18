package retry

import "context"

// idempotentKey is the context key Idempotent/isIdempotent use.
type idempotentKey struct{}

// Idempotent marks ctx so a mutating call made with it becomes eligible for
// the retry policy despite not being a recognized read method (methods.go).
// Call sites must only use this when repeating the call is actually safe:
// e.g. PostOrder carrying a client order_id that was persisted to the intent
// ledger before the network send, or CancelOrder, which converges when
// repeated (docs/plan-tinvest-cli.md §9: "Mutations → retried only when
// idempotent"). This marker, and the read/mutation split it enables, is the
// CLI's own addition — the upstream SDK's retry package has no idempotency
// concept at all and retries indiscriminately by gRPC code
// (docs/sdk-spike.md §1).
func Idempotent(ctx context.Context) context.Context {
	return context.WithValue(ctx, idempotentKey{}, true)
}

// isIdempotent reports whether ctx was marked via Idempotent.
func isIdempotent(ctx context.Context) bool {
	v, _ := ctx.Value(idempotentKey{}).(bool)
	return v
}
