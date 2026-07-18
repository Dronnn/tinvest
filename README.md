# tinvest

A command-line interface for the [T-Invest API](https://developer.tbank.ru/invest/intro/intro) (T-Bank brokerage). `tinvest` is a thin, predictable broker adapter: it retrieves data, executes requested operations, validates inputs, and returns structured JSON — nothing more. It performs no market analysis and makes no trading decisions, which makes it equally suitable for shell scripts, automation, and AI agents.

> **Status: early development.** The command surface and output contract are not stable yet.

## Design principles

- **Stateless.** Every invocation reads the token from the environment, talks gRPC to the broker, prints a result, and exits. No daemons, no background processes.
- **Machine-first output.** A uniform JSON envelope with a stable `schema_version`, machine-readable errors, and a fixed exit-code contract. Monetary values are decimal strings — never floats.
- **Reliability over convenience.** Client-side idempotency keys for orders, an explicit unknown-state protocol with reconciliation, and no automatic retries where duplicates could be created.
- **Vendored contracts.** The gRPC contracts are vendored and pinned (see `proto/VERSION.md`); generated code is committed. `make proto` reproduces it byte-for-byte with pinned tool versions — no system-wide installs required beyond Go itself.

## Requirements

- Go 1.26+
- A T-Invest API token (issued in the T-Investments app settings). The token is read from the `TINVEST_TOKEN` environment variable and is never accepted as a command-line argument.

## Build

```sh
make build     # compile
make test      # run tests
make lint      # golangci-lint via pinned go run
make proto     # regenerate gRPC stubs from vendored protos
```

## Usage

The command surface is under active development. Currently available:

```sh
tinvest version         # CLI version, pinned contract version, schema version
tinvest token check     # validate the token; report user info, accounts, access levels
tinvest accounts list   # list accounts visible to the token
tinvest instruments …   # search / get / list instruments
tinvest quotes last …   # last / close prices
tinvest orderbook get … # market depth
```

### Orders

The order group is idempotent and journaled: every placement writes a client
`order_id` and a write-ahead intent record before the network send, so a crash
or a timed-out send never issues a duplicate and can always be reconciled.

```sh
# Place a limit order (idempotent; order_id generated if omitted).
tinvest orders place --account <id> --instrument <uid|FIGI|TICKER@CLASSCODE> \
    --direction buy --quantity 1 --type limit --price 250.5 [--tif day|ioc|fok] \
    [--order-id <uuid>] [--async] [--dry-run] [--yes]

# Same request as a JSON document (mirrors the flags; unknown fields rejected).
echo '{"instrument":"<uid>","direction":"buy","quantity":1,"type":"limit","price":"250.5"}' \
    | tinvest orders place --account <id> --input -

tinvest orders preview  --account <id> --instrument <id> --direction buy --quantity 1 --price 250.5
tinvest orders max-lots --account <id> --instrument <id> [--price 250.5]
tinvest orders get <order-id> --account <id> [--request-id]
tinvest orders list --account <id>
tinvest orders cancel <order-id> --account <id>
tinvest orders replace <order-id> --account <id> --quantity 2 [--price 251]
tinvest orders wait <order-id> --account <id> [--timeout 60s]   # block until terminal
tinvest orders reconcile --account <id>                         # resolve every unconfirmed intent
```

`--dry-run` validates, previews cost (`GetOrderPrice`), and reports max lots
without placing anything or touching the journal. `--async` uses `PostOrderAsync`
and returns a `trade_intent_id`. Mutating commands require `--account` (or a
profile default) — the CLI never guesses.

Placement guardrails live in an optional **policy file** referenced from a
profile as `policy_file`. A breach fails with exit 2, code `POLICY`, before any
network call:

```toml
# policy.toml
allowed_instruments  = []          # allowlist of uids/FIGIs/TICKER@CLASSCODE; empty = allow all
max_lots_per_order   = 100
max_notional_per_order = "100000"  # requires notional_currency
notional_currency    = "rub"
max_open_orders      = 25
allow_market_orders  = false       # market/bestprice opt-in
allow_shorts         = false       # short opt-in (position check is a TODO for M2)
kill_switch_file     = "~/.config/tinvest/KILL"  # its presence blocks all mutations
```

Global flags: `--profile <name>` (config profile), `--account <id>`, `-o json|table`, `--token-file <path>`, `--timeout <duration>` (per-call deadline, default 10s), `--sandbox` (shortcut for the sandbox endpoint).

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

Planned command groups: `portfolio`, `positions`, `balance`, `candles`, `stop-orders`, `operations`, `stream`, `sandbox`.

## Disclaimer

This is an independent open-source project, not affiliated with or endorsed by T-Bank. Trading involves risk; you are solely responsible for any operations executed through this tool. Use a sandbox or read-only token whenever possible.
