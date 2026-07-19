# Agent integration

Use `tinvest` as a command-per-operation interface. Run broker operations with
`-o json`, decode the JSON envelope, and never parse table output or prose.
Utility output such as `--help` and `completion` is prose or a shell script,
not a JSON envelope.

The complete per-command flag reference is [COMMANDS.md](COMMANDS.md),
generated from the binary's `--help` output; `tinvest [command] --help` serves
the same text at runtime. This file defines the semantics those commands share.

## Unary JSON contract

Every non-stream broker operation returns one envelope:

```json
{
  "ok": true,
  "data": {},
  "meta": {
    "elapsed_ms": 0,
    "contract": "1.49",
    "schema_version": "0.1"
  }
}
```

Failures have `ok: false`, omit `data`, and provide `error` plus `meta`.
Branch on the process exit code and machine fields, not `error.message`. Check
`meta.schema_version` before decoding command-specific `data`; reject or route
unsupported schema versions instead of guessing.

| Exit | Meaning | Required caller action |
| ---: | --- | --- |
| 0 | Success | Consume `data`. |
| 1 | Internal failure, or a reconcile that could not fully resolve | Do not retry blindly. Record the envelope and tracking ID, then stop or escalate. For a `reconcile` command see the reconcile note below. |
| 2 | Usage or policy rejection | Fix the request, or ask a human to change policy. Do not retry unchanged. |
| 3 | Authentication or permission failure | Repair credentials or access before retrying. |
| 4 | Rate limited | Wait at least `error.retry_after_ms` when present, then retry according to operation semantics. |
| 5 | Broker rejected the request | Treat the result as confirmed. Fix the request or broker-side state before retrying. |
| 6 | Network or timeout failure | Reads may be retried. Preserve the same client order ID for any retryable order operation. |
| 7 | Mutation sent, outcome unknown | **MUST run the command in `error.reconcile.command` (normally `tinvest orders reconcile` or `tinvest stop-orders reconcile`) before any retry.** Never submit a replacement operation until reconciliation resolves the intent. |

## Mutations and guardrails

- Give each intended order placement one stable client `order_id` with
  `--order-id`; successful output exposes it as `client_order_id`. Reuse that
  ID when retrying the same intended operation. Never reuse it for a new
  operation.
- The profile's `policy_file` is the human-set guardrail layer. It can restrict
  instruments, lots, notional, open orders, market orders, and all mutations
  through a kill-switch file. Automation must not edit, bypass, or
  disable it.
- Use `orders place --dry-run` to validate and obtain broker price/max-lot
  previews without placing an order or writing an intent. `stop-orders place
  --dry-run` performs local validation only and makes no network call.
- Run one command for one operation. Do not combine several intended mutations
  behind one shell command whose partial outcome cannot be attributed.
- `orders reconcile` and `stop-orders reconcile` exit **0** only when every
  intent resolved cleanly (each outcome is `placed` or `not-placed`). If any
  outcome is `indeterminate`, `error`, `unresolved`, or `ambiguous`, the command
  exits **1** and sets `data.unresolved_count`. The envelope still reports
  `ok: true` — reconcile itself ran — but the non-zero exit means "state is still
  in doubt": inspect each `outcomes[].outcome`/`error`, act on the guidance, and
  re-run before assuming any intent is settled. `foreign` and `profile-mismatch`
  outcomes are deliberate cross-command / cross-profile skips and do not, on
  their own, force the non-zero exit. A `placed` stop-order outcome carries a
  `note` that its correlation is heuristic (stop orders have no client id).

## NDJSON streams

`tinvest stream ...` does not use the unary envelope. It emits one complete
JSON object per line. Every event has top-level `type`, `schema_version`, and
RFC 3339 UTC `time`; account streams can also carry `account_id`. Decode lines
independently and check `schema_version` on every event.

Event types:

- lifecycle: `connected`, `disconnected`, `resubscribed`, `lagging`;
- market data: `candle`, `orderbook`, `trade`, `last_price`, `info`,
  `open_interest`;
- account/order data: `portfolio`, `positions_snapshot`, `positions`,
  `order_trade`;
- protocol/control: `subscription`, `control`, `ping`, `unknown`;
- reconciliation and failure: `snapshot`, `error`.

`connected.data.subscriptions` is the number of requested data stream kinds
(subscription request batches), not setup/control requests. `subscription`
contains a broker subscription acknowledgement. `control` identifies an empty
broker control frame whose protobuf payload oneof is unset. An `unknown` event
always names the unhandled protobuf case in
`data.protobuf_oneof_case`; record it and do not infer a data shape.

For order-book streams, treat every `snapshot` as authoritative replacement
state. A snapshot is emitted after each connection or reconnection; buffered
order-book frames at or before its timestamp are discarded. Do not apply
pre-gap local state after a reconnect. A final clean shutdown is
`disconnected` with `data.reason: "shutdown"` and `data.final: true`.

## Rate limiting

Unary calls use process-local method-group token buckets. The limiter waits up
to two seconds; a longer required wait becomes exit 4. Every automatic retry
consumes another token. A best-effort tariff refresh can replace static limits
for the current process. `--no-rate-limit` disables only the local limiter and
refresh; broker limits still apply.

Stream replay sends at most 100 setup/subscription requests per rolling minute,
and market-data streams allow at most 300 logical subscriptions. Reconnect
replay can therefore wait before the next `connected` event.

## Recommended invocation pattern

1. Run exactly one operation with `-o json`.
2. Decode the envelope from stdout; never parse table output.
3. Verify `meta.schema_version`.
4. Branch on the exit code table above, using structured `error` fields.
5. Preserve `client_order_id`, reconciliation instructions, and tracking IDs in
   the caller's operation record.
