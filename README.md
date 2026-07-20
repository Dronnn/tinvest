# tinvest

A command-line interface for the [T-Invest API](https://developer.tbank.ru/invest/intro/intro) (T-Bank brokerage). `tinvest` is a thin, predictable broker adapter: it retrieves data, executes requested operations, validates inputs, and returns structured JSON — nothing more. It performs no market analysis and makes no trading decisions, which makes it equally suitable for shell scripts, automation, and AI agents.

## Design principles

- **Stateless per invocation, no daemons.** Every invocation reads the token from the environment, talks gRPC to the broker, prints a result, and exits — there are no background processes. The one piece of local state is the on-disk intent journal (write-ahead ledger) that mutating order commands use for idempotency and reconciliation; each command still runs and exits on its own.
- **Machine-first output.** A uniform JSON envelope with a stable `schema_version`, machine-readable errors, and a fixed exit-code contract. Monetary values are decimal strings — never floats.
- **Reliability over convenience.** Client-side idempotency keys for orders, an explicit unknown-state protocol with reconciliation, and no automatic retries where duplicates could be created.
- **Vendored contracts.** The gRPC contracts are vendored and pinned (see `proto/VERSION.md`); generated code is committed. `make proto` reproduces it byte-for-byte with pinned tool versions — no system-wide installs required beyond Go itself.

## Requirements

- Go 1.26+
- A T-Invest API token (issued in the T-Investments app settings). The token is read from the `TINVEST_TOKEN` environment variable and is never accepted as a command-line argument.

### Russian CA certificates

T-Bank's API endpoints present certificates chained to the **Russian Trusted Sub CA** (Минцифры / Ministry of Digital Development), which most operating systems do not trust by default — connections fail with a certificate-verification error unless the OS already has this chain installed, which is common inside Russia but usually not elsewhere.

`tinvest` never touches your system's trust store. Instead, point it at a CA bundle of your own:

1. Download the official root and intermediate certificates (PEM format) from **https://www.gosuslugi.ru/crt**: the **Russian Trusted Root CA** and the **Russian Trusted Sub CA**.
2. Concatenate both into a single bundle file, e.g.:
   ```sh
   mkdir -p ~/.config/tinvest
   # ensure a newline separates the two certificates (the downloads lack a trailing newline)
   { cat russian_trusted_root_ca.pem; echo; cat russian_trusted_sub_ca.pem; echo; } | tr -d '\r' > ~/.config/tinvest/russian-trusted-ca.pem
   ```
3. Point `tinvest` at it, either in the profile:
   ```toml
   [profiles.main]
   ca_file = "~/.config/tinvest/russian-trusted-ca.pem"
   ```
   or via the environment (wins over the profile):
   ```sh
   export TINVEST_CA_FILE=~/.config/tinvest/russian-trusted-ca.pem
   ```

When `ca_file`/`TINVEST_CA_FILE` is set, `tinvest` verifies the server certificate against that bundle instead of the system trust store — hostname verification is unaffected. There is no option to disable certificate verification; it is not offered.

## Build

```sh
make build     # compile
make test      # run tests
make lint      # golangci-lint via pinned go run
make proto     # regenerate gRPC stubs from vendored protos
```

An opt-in live-integration suite against the real T-Invest sandbox lives in
`test/e2e-live/` (build tag `e2elive`; requires `TINVEST_TOKEN`; never touches
production): `make test-live`. See `test/e2e-live/README.md`.

## Install from a release

Download the archive for your OS and architecture plus `checksums.txt` from
[GitHub Releases](https://github.com/Dronnn/tinvest/releases), verify the
archive checksum, extract it, and place `tinvest` on your `PATH`.

## Shell completions

The built-in `completion` command generates scripts for bash, zsh, and fish:

```sh
source <(tinvest completion bash)        # bash, current session
source <(tinvest completion zsh)         # zsh, after compinit
tinvest completion fish | source         # fish, current session
```

Use `tinvest completion <shell> --help` for persistent installation paths.

## Usage

The full per-command flag reference is [COMMANDS.md](COMMANDS.md) (generated
via `make docs-commands`); the machine contract for AI agents — envelope,
exit codes, reconcile protocol — is [AGENTS.md](AGENTS.md).

Command groups:

```sh
tinvest version         # CLI version, pinned contract version, schema version
tinvest token check     # validate the token; report user info, accounts, access levels
tinvest accounts list   # list accounts visible to the token
tinvest instruments …   # search / get / list instruments
tinvest quotes last …   # last / close prices
tinvest orderbook get … # market depth
tinvest portfolio get   # portfolio totals, yield, and holdings
tinvest positions get   # money, securities, futures, options, and blocked amounts
tinvest balance get     # withdraw limits summarized by currency
tinvest operations list # cursor-paginated operations; --all follows every page
tinvest trades list     # executed trades flattened from operations
tinvest candles …       # auto-windowed candles / authenticated yearly zip download
tinvest research …      # news / fundamentals / forecasts / insider deals
tinvest user …          # tariff limits / account margin attributes
tinvest signals …       # signal strategies / signals
tinvest orders …        # place / list / cancel / replace / wait / reconcile
tinvest stop-orders …   # take-profit / stop-loss / stop-limit, never auto-retried
tinvest sandbox …       # sandbox account open / close / accounts / topup
tinvest stream …        # resilient market/account/order streams as NDJSON
```

### Orders

The order group is idempotent and journaled: every placement writes a client
`order_id` and a write-ahead intent record before the network send, so a crash
or timed-out send is never retried under a new key. Regular order intents can
be looked up by that key; reconciliation leaves profile mismatches and other
indeterminate cases unresolved instead of guessing.

```sh
# Place a limit order (idempotent; order_id generated if omitted).
tinvest orders place --account <id> --instrument <uid|FIGI|TICKER@CLASSCODE> \
    --direction buy --quantity 1 --type limit --price 250.5 [--tif day|ioc|fok] \
    [--order-id <uuid>] [--async] [--confirm-margin-trade] [--dry-run] [--yes]

# Same request as a JSON document (mirrors the flags; unknown fields rejected).
echo '{"instrument":"<uid>","direction":"buy","quantity":1,"type":"limit","price":"250.5"}' \
    | tinvest orders place --account <id> --input -

tinvest orders preview  --account <id> --instrument <id> --direction buy --quantity 1 --price 250.5
tinvest orders max-lots --account <id> --instrument <id> [--price 250.5]
tinvest orders get <order-id> --account <id> [--request-id]
tinvest orders list --account <id>
tinvest orders cancel <order-id> --account <id>
tinvest orders replace <order-id> --account <id> --quantity 2 [--price 251] [--confirm-margin-trade]
tinvest orders wait <order-id> --account <id> [--timeout 60s]   # block until terminal
tinvest orders reconcile --account <id>                         # reconcile regular-order intents for the active profile/endpoint
```

`--dry-run` validates, previews cost (`GetOrderPrice`), and reports max lots
without placing anything or touching the journal. `--async` uses `PostOrderAsync`
and returns a `trade_intent_id`. Mutating commands require `--account` (or a
profile default) — the CLI never guesses.

Placement guardrails live in an optional **policy file** referenced from a
profile as `policy_file`. Locally decidable breaches fail with exit 2, code
`POLICY`, before any broker request. Instrument allowlist/notional checks and
the open-order cap require read-only broker lookups, but still run before the
placement or replacement mutation:

```toml
# policy.toml
allowed_instruments  = []          # allowlist of uids/FIGIs/TICKER@CLASSCODE; empty = allow all
max_lots_per_order   = 100
max_notional_per_order = "100000"  # requires notional_currency; LIMIT orders only
notional_currency    = "rub"
max_open_orders      = 25
allow_market_orders  = false       # market/bestprice opt-in
allow_shorts         = false       # short opt-in (position check is a TODO for M2)
kill_switch_file     = "~/.config/tinvest/KILL"  # presence (or an unreadable path) blocks all mutations
```

`max_notional_per_order` applies only to **limit** orders: market and bestprice
orders have no price at the local validation stage, so their notional cannot be
computed without a quote and the cap does not apply — `allow_market_orders` is
their guardrail. The `kill_switch_file` check fails **closed**: if the path
exists (switch engaged) *or* cannot be stat-ed (permission/I-O error), the
mutation is blocked with a `POLICY` error.

`max_open_orders` caps active orders **per order book**: `orders place` counts
active regular orders (`GetOrders`), and `stop-orders place` counts active stop
orders (`GetStopOrders --status active`), each against the same limit. Regular
and stop orders are therefore counted separately — a full regular book does not
block a stop order and vice versa. Both checks need a read-only lookup and run
before the placement mutation; a breach is exit 2, code `POLICY`.

### Stop orders

`stop-orders` covers take-profit, stop-loss, and stop-limit (incl. trailing
take-profit). It shares the ledger/policy/dry-run machinery with `orders`,
with one deliberate difference: **placement is never auto-retried.** The
current contract has a required `order_id` idempotency field on
`PostStopOrder`, but its dedup retention is undocumented, so a timed-out send
always surfaces as exit 7 with a reconcile hint instead of being retried.

```sh
tinvest stop-orders place --account <id> --instrument <uid|FIGI|TICKER@CLASSCODE> \
    --direction buy --quantity 1 --type stop-loss --stop-price 240 \
    [--price 239.5]                        # required only for --type stop-limit
    [--expiration gtc|gtd] [--expire-date <RFC3339>] \
    [--exchange-order-type market|limit] \
    [--take-profit-type regular|trailing] \
    [--trailing-indent 1 --trailing-indent-type absolute \
     --trailing-spread 0.5 --trailing-spread-type absolute] \
    [--order-id <uuid>] [--dry-run] [--yes]

tinvest stop-orders list --account <id> [--status all|active|executed|canceled|expired]
tinvest stop-orders cancel <stop-order-id> --account <id>
tinvest stop-orders reconcile --account <id>   # reconcile stop intents for the active profile/endpoint
```

`--exchange-order-type`, `--take-profit-type`, and trailing parameters are
take-profit-only.

`--dry-run` is local validation only — stop orders have no
`GetOrderPrice`/`GetMaxLots` equivalent to preview against, so nothing is sent
and no network call is made. `GetStopOrders` does not echo the client
`order_id`, so `reconcile` requests all statuses and matches every available
request field, including order type, child-order type/price, expiry, trailing
parameters, and a creation window around the journaled intent time. Zero,
multiple, legacy, or otherwise uncertain matches remain unresolved with a
manual-check explanation.

### Sandbox

`sandbox` manages sandbox (paper-trading) accounts. These commands **always
target the sandbox endpoint**, overriding the active profile if needed (with
a warning on stderr) — a sandbox mutation must never reach production. They
still respect the policy kill switch (a mutation is a mutation), but write no
ledger entry (account management isn't an order intent).

```sh
tinvest sandbox open [--name <name>]
tinvest sandbox close <account-id>
tinvest sandbox accounts
tinvest sandbox topup --account <id> --amount 10000 [--currency rub]
```

### Read-only breadth

Times are RFC 3339. Instrument arguments accept UID, FIGI, or
`TICKER@CLASSCODE` and are resolved to UID before the data call.

```sh
tinvest portfolio get --account <id>
tinvest positions get --account <id>
tinvest balance get --account <id>
tinvest operations list --account <id> --from 2026-01-01T00:00:00Z --to 2026-02-01T00:00:00Z --limit 250 --all
tinvest trades list --account <id> --instrument SBER@TQBR --all
tinvest instruments list --type share
tinvest instruments dividends SBER@TQBR --from 2026-01-01T00:00:00Z --to 2027-01-01T00:00:00Z
tinvest instruments coupons <bond-uid> --from 2026-01-01T00:00:00Z --to 2027-01-01T00:00:00Z
tinvest instruments accrued-interest <bond-uid> --from 2026-01-01T00:00:00Z --to 2026-02-01T00:00:00Z
tinvest instruments schedules --exchange MOEX --from 2026-01-01T00:00:00Z --to 2026-01-08T00:00:00Z
tinvest instruments trading-status SBER@TQBR
tinvest research news --limit 100 [--cursor <news-id>]
tinvest research fundamentals --asset <asset-uid> [--instrument SBER@TQBR]
tinvest research forecast --instrument SBER@TQBR
tinvest research consensus --page-number 0 --limit 100
tinvest research insider-deals --instrument SBER@TQBR --limit 100 [--cursor <next-cursor>]
tinvest candles get SBER@TQBR --interval 1h --from 2026-01-01T00:00:00Z --to 2026-07-01T00:00:00Z
tinvest candles download SBER@TQBR --year 2025 --out ./history
tinvest user tariff
tinvest user margin --account <id>
tinvest signals strategies
tinvest signals list --strategy <strategy-id>
```

### Streams

Streams always write NDJSON to stdout: exactly one complete JSON object per
line, without the normal unary response envelope. Every event starts with
`type` and carries `schema_version` plus an RFC 3339 UTC `time`. Data frames
use types such as `candle`, `orderbook`, `trade`, `last_price`, `portfolio`,
`positions`, and `order_trade`; connection state is explicit through
`connected`, `disconnected`, `resubscribed`, and `lagging` frames. Broker
subscription acknowledgements use `subscription`; empty broker control frames
use `control`. Unsupported protobuf frames use `unknown` with the oneof case
named in `data.protobuf_oneof_case`.

```sh
tinvest stream marketdata --instrument SBER@TQBR --candles=1m --trades --last-price
tinvest stream marketdata --instrument <uid> --orderbook=20 --info
tinvest stream portfolio --account <id>
tinvest stream positions --account <id>
tinvest stream orders --account <id>
```

Dropped or silent streams reconnect with capped jittered exponential backoff.
Market-data subscriptions are replayed from a de-duplicated registry; every
order-book connection/reconnection also fetches `GetOrderBook` and emits a
`snapshot` event so consumers can replace potentially gapped local state;
buffered order-book frames older than that snapshot are discarded. Replay is
capped at 100 subscription requests per rolling minute. Server pings are
requested every 10 seconds and a 30-second no-data-or-ping watchdog forces
reconnection. SIGINT/SIGTERM emits a final `disconnected` event with reason
`shutdown`, flushes it, and exits successfully.

Unary RPCs share process-local token buckets by broker method group. Static
defaults are 600/min market data, 300/min operations and sandbox, 200/min
instruments (15/min for the heavy list methods), 100/min orders/users/signals,
and 50/min stop orders, with a two-second maximum local wait. Every retry
attempt consumes a token. At connection startup the CLI makes a one-second,
best-effort `GetUserTariff` refresh; static defaults remain active if it is
unavailable. `--no-rate-limit` disables both this client-side guardrail and
the refresh; broker-side limits still apply.

Global flags: `--profile <name>` (config profile), `--account <id>`, `-o json|table`, `--token-file <path>`, `--timeout <duration>` (per-call deadline, default 10s), `--sandbox` (shortcut for the sandbox endpoint), `--no-rate-limit` (disable local unary throttling).

Output is a uniform JSON envelope (`{"ok":…,"data":…,"meta":{…}}`) with a stable `schema_version`; errors carry a machine-readable classification and map to a fixed exit-code contract (`0` ok, `1` internal, `2` usage, `3` auth, `4` rate-limited, `5` rejected by broker, `6` network/timeout, `7` mutation sent but unconfirmed). JSON is the default when stdout is not a terminal; `-o` or `TINVEST_OUTPUT` overrides unconditionally.

Configuration profiles live in `~/.config/tinvest/config.toml` (`XDG_CONFIG_HOME` respected):

```toml
default_profile = "main"

[profiles.main]
endpoint = "prod"        # "prod", "sandbox", or host:port
account_id = "…"
output = "json"
token_file = "~/.config/tinvest/token"
policy_file = "~/.config/tinvest/policy.toml"   # optional pre-trade guardrails
```

Token resolution order: `--token-file` flag, then `TINVEST_TOKEN`, then the profile's `token_file`.

## Use as a Go library

The read-only surface of the CLI is also available as an importable package. It
wraps the same broker and transport layers over one shared gRPC connection with
the identical interceptor stack — Bearer auth, per-call deadlines, the
idempotency-aware retry policy, client-side rate limiting, and tracking-id
capture.

```sh
go get github.com/Dronnn/tinvest@latest
```

The library surface was introduced after `v1.1.0`, so the first importable tag
is the next release; until then, pin the module to a local checkout with a
`replace` directive in the consumer's `go.mod`:

```
require github.com/Dronnn/tinvest v0.0.0

replace github.com/Dronnn/tinvest => ../invest
```

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Dronnn/tinvest"
)

func main() {
	ctx := context.Background()

	client, err := tinvest.New(ctx, tinvest.Config{Token: "t.your_token_here"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Identifiers accept an instrument_uid, a FIGI, or a TICKER@CLASSCODE pair.
	inst, err := client.Resolve(ctx, "SBER@TQBR")
	if err != nil {
		log.Fatal(err)
	}

	prices, err := client.LastPrices(ctx, inst.GetUid())
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range prices {
		fmt.Printf("%s: %s\n", inst.GetTicker(), tinvest.QuotationString(p.GetPrice()))
	}
}
```

Methods cover instrument resolution and search, per-type instrument lists,
last/close prices, order books, candles (with the CLI's automatic range
windowing), trading status, dividends, coupons, accrued interest, trading
schedules, and the research surface (news, fundamentals, forecasts, consensus,
insider deals). They return the generated protobuf types from
`github.com/Dronnn/tinvest/pb/investapi` or small result structs; use
`tinvest.QuotationString` and `tinvest.MoneyString` to render `Quotation`/
`MoneyValue` as exact decimal strings. Broker failures are returned as
`*tinvest.APIError`, which exposes the gRPC status code and the broker's
`x-tracking-id` (via `errors.As`).

The `tinvest.Client` surface is intentionally **read-only**: order placement,
stop orders, sandbox mutations, streaming, and the intent ledger are not exposed
by `Client` and remain CLI-only by design. That guarantee covers `Client` only —
the generated `github.com/Dronnn/tinvest/pb/investapi` package is the full gRPC
contract and exports the raw service clients, including mutating RPCs; calling
those directly bypasses this project's guardrails and is at your own risk.

Importing the library does not pull the CLI framework into your build: `cobra`
and `pflag` stay in the module for the `tinvest` command but are not linked into
consumers of the `github.com/Dronnn/tinvest` package.

## License

Licensed under the Apache License, Version 2.0. Copyright 2026 Andreas Maier.
See [LICENSE](LICENSE). Third-party provenance and attribution are recorded in
[NOTICE](NOTICE) and [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).

## Disclaimer

This is an independent open-source project, not affiliated with or endorsed by T-Bank. Trading involves risk; you are solely responsible for any operations executed through this tool. Use a sandbox or read-only token whenever possible.
