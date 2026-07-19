# tinvest command reference

Generated from the binary's `--help` output (`make docs-commands`);
do not edit by hand. The semantic contract — JSON envelope, exit
codes, reconcile protocol, NDJSON streams, guardrails — is in
[AGENTS.md](AGENTS.md).

```
tinvest is a stateless command-line adapter for the T-Bank Invest gRPC API:
validate, transmit, report. Machine-first JSON output with a stable envelope
and exit-code contract.

Usage:
  tinvest [command]

Available Commands:
  accounts    Brokerage accounts
  balance     Withdrawable and blocked money
  candles     Historic candles and bulk archives
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  instruments Instrument reference data
  operations  Cursor-paginated account operations
  orderbook   Order book (market depth)
  orders      Place, track, cancel, and reconcile orders
  portfolio   Portfolio totals and holdings
  positions   Account positions and blocked quantities
  quotes      Market quotes
  research    News, fundamentals, forecasts, and insider activity
  sandbox     Manage sandbox accounts (always targets the sandbox endpoint)
  signals     Analyst and technical signals
  stop-orders Place, list, cancel, and reconcile stop orders (take-profit, stop-loss, stop-limit)
  stream      Resilient broker streams as NDJSON
  token       API token utilities
  trades      Executed trades from operation history
  user        User tariff and account attributes
  version     Print CLI, contract, and schema versions

Flags:
      --account string      account id for account-scoped commands
  -h, --help                help for tinvest
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest [command] --help" for more information about a command.
```

## tinvest accounts

```
Brokerage accounts

Usage:
  tinvest accounts [command]

Available Commands:
  list        List accounts visible to the token

Flags:
  -h, --help   help for accounts

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest accounts [command] --help" for more information about a command.
```

### tinvest accounts list

```
List accounts visible to the token

Usage:
  tinvest accounts list [flags]

Flags:
  -h, --help   help for list

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest balance

```
Withdrawable and blocked money

Usage:
  tinvest balance [command]

Available Commands:
  get         Get withdraw limits summarized by currency

Flags:
  -h, --help   help for balance

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest balance [command] --help" for more information about a command.
```

### tinvest balance get

```
Get withdraw limits summarized by currency

Usage:
  tinvest balance get [flags]

Flags:
  -h, --help   help for get

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest candles

```
Historic candles and bulk archives

Usage:
  tinvest candles [command]

Available Commands:
  download    Download a yearly bulk candle-history zip
  get         Get historic candles with automatic range windowing

Flags:
  -h, --help   help for candles

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest candles [command] --help" for more information about a command.
```

### tinvest candles download

```
Download a yearly bulk candle-history zip

Usage:
  tinvest candles download <id> [flags]

Flags:
  -h, --help         help for download
      --no-cache     bypass the local instrument cache
      --out string   output directory (default ".")
      --year int     four-digit archive year (required)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest candles get

```
Get historic candles with automatic range windowing

Usage:
  tinvest candles get <id> [flags]

Flags:
      --from string       period start as RFC3339 (required)
  -h, --help              help for get
      --interval string   1m, 2m, 3m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 1d, 1w, or 1M
      --no-cache          bypass the local instrument cache
      --to string         period end as RFC3339 (required)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest instruments

```
Instrument reference data

Usage:
  tinvest instruments [command]

Available Commands:
  accrued-interest List accrued bond interest in a time range
  coupons          List bond coupon events in a time range
  dividends        List dividend events in a time range
  get              Resolve an instrument identifier to its full reference record
  list             List base instruments by type
  schedules        Get exchange trading schedules
  search           Free-text instrument search
  trading-status   Get current trading and order availability

Flags:
  -h, --help   help for instruments

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest instruments [command] --help" for more information about a command.
```

### tinvest instruments accrued-interest

```
List accrued bond interest in a time range

Usage:
  tinvest instruments accrued-interest <id> [flags]

Flags:
      --from string   period start as RFC3339 (required)
  -h, --help          help for accrued-interest
      --no-cache      bypass the local instrument cache
      --to string     period end as RFC3339 (required)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest instruments coupons

```
List bond coupon events in a time range

Usage:
  tinvest instruments coupons <id> [flags]

Flags:
      --from string   period start as RFC3339 (required)
  -h, --help          help for coupons
      --no-cache      bypass the local instrument cache
      --to string     period end as RFC3339 (required)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest instruments dividends

```
List dividend events in a time range

Usage:
  tinvest instruments dividends <id> [flags]

Flags:
      --from string   period start as RFC3339 (required)
  -h, --help          help for dividends
      --no-cache      bypass the local instrument cache
      --to string     period end as RFC3339 (required)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest instruments get

```
Resolve an instrument identifier to its full reference record

Usage:
  tinvest instruments get <instrument_uid|figi|TICKER@CLASSCODE> [flags]

Flags:
  -h, --help       help for get
      --no-cache   bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest instruments list

```
List base instruments by type

Usage:
  tinvest instruments list [flags]

Flags:
  -h, --help          help for list
      --type string   share, bond, etf, currency, future, or option

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest instruments schedules

```
Get exchange trading schedules

Usage:
  tinvest instruments schedules [flags]

Flags:
      --exchange string   exchange name (default: all exchanges)
      --from string       period start as RFC3339 (required)
  -h, --help              help for schedules
      --to string         period end as RFC3339 (required)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest instruments search

```
Free-text instrument search

Usage:
  tinvest instruments search <text> [flags]

Flags:
  -h, --help   help for search

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest instruments trading-status

```
Get current trading and order availability

Usage:
  tinvest instruments trading-status <id> [flags]

Flags:
  -h, --help       help for trading-status
      --no-cache   bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest operations

```
Cursor-paginated account operations

Usage:
  tinvest operations [command]

Available Commands:
  list        List account operations with cursor paging

Flags:
  -h, --help   help for operations

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest operations [command] --help" for more information about a command.
```

### tinvest operations list

```
List account operations with cursor paging

Usage:
  tinvest operations list [flags]

Flags:
      --all                 follow cursors and return all pages
      --cursor string       cursor from a previous response
      --from string         period start as RFC3339 (optional)
  -h, --help                help for list
      --instrument string   instrument id filter: uid, FIGI, or TICKER@CLASSCODE
      --limit int32         operations per page (3 through 1000) (default 100)
      --to string           period end as RFC3339 (optional)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest orderbook

```
Order book (market depth)

Usage:
  tinvest orderbook [command]

Available Commands:
  get         Order book for an instrument

Flags:
  -h, --help   help for orderbook

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest orderbook [command] --help" for more information about a command.
```

### tinvest orderbook get

```
Order book for an instrument

Usage:
  tinvest orderbook get <id> [flags]

Flags:
      --depth int32   order book depth: 1, 10, 20, 30, 40, or 50 (default 20)
  -h, --help          help for get
      --no-cache      bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest orders

```
Place, track, cancel, and reconcile orders

Usage:
  tinvest orders [command]

Available Commands:
  cancel      Cancel an active order (idempotent)
  get         Order state by exchange order id (or --request-id for the client key)
  list        List active orders on the account
  max-lots    Maximum tradable lots for an instrument (GetMaxLots)
  place       Place an order (idempotent, journaled)
  preview     Pre-trade cost and commission (GetOrderPrice), places nothing
  reconcile   Resolve every unconfirmed regular-order intent in the journal against the broker
  replace     Replace an active order's price and/or quantity
  wait        Block until an order reaches a terminal state (filled/cancelled/rejected)

Flags:
  -h, --help   help for orders

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest orders [command] --help" for more information about a command.
```

### tinvest orders cancel

```
Cancel an active order (idempotent)

Usage:
  tinvest orders cancel <order-id> [flags]

Flags:
  -h, --help   help for cancel

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders get

```
Order state by exchange order id (or --request-id for the client key)

Usage:
  tinvest orders get <order-id> [flags]

Flags:
  -h, --help         help for get
      --request-id   interpret the id as the client idempotency key, not the exchange id

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders list

```
List active orders on the account

Usage:
  tinvest orders list [flags]

Flags:
  -h, --help   help for list

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders max-lots

```
Maximum tradable lots for an instrument (GetMaxLots)

Usage:
  tinvest orders max-lots [flags]

Flags:
  -h, --help                help for max-lots
      --instrument string   instrument id: uid, FIGI, or TICKER@CLASSCODE
      --no-cache            bypass the local instrument cache
      --price string        price as a decimal string (refines buy limits)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders place

```
Place an order (idempotent, journaled)

Usage:
  tinvest orders place [flags]

Flags:
      --async                  place asynchronously (PostOrderAsync)
      --confirm-margin-trade   confirm an order that may create an uncovered position
      --direction string       buy or sell
      --dry-run                validate and preview only; place nothing
  -h, --help                   help for place
      --input string           read the full request as JSON from a file or - for stdin
      --instrument string      instrument id: uid, FIGI, or TICKER@CLASSCODE
      --no-cache               bypass the local instrument cache
      --order-id string        client idempotency key (UUID); generated if omitted
      --price string           limit price as a decimal string (required for limit)
      --quantity int           number of lots (positive)
      --tif string             time in force: day, ioc, or fok
      --type string            limit, market, or bestprice
      --yes                    confirm the mutation (accepted for symmetry; no interactive prompt)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders preview

```
Pre-trade cost and commission (GetOrderPrice), places nothing

Usage:
  tinvest orders preview [flags]

Flags:
      --direction string    buy or sell
  -h, --help                help for preview
      --instrument string   instrument id: uid, FIGI, or TICKER@CLASSCODE
      --no-cache            bypass the local instrument cache
      --price string        price as a decimal string (required for limit)
      --quantity int        number of lots (positive)
      --type string         limit, market, or bestprice (default "limit")

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders reconcile

```
Resolve every unconfirmed regular-order intent in the journal against the broker

Usage:
  tinvest orders reconcile [flags]

Flags:
  -h, --help   help for reconcile

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders replace

```
Replace an active order's price and/or quantity

Usage:
  tinvest orders replace <order-id> [flags]

Flags:
      --confirm-margin-trade   confirm a replacement that may create an uncovered position
  -h, --help                   help for replace
      --price string           new limit price as a decimal string
      --quantity int           new number of lots (positive)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest orders wait

```
Block until an order reaches a terminal state (filled/cancelled/rejected)

Usage:
  tinvest orders wait <order-id> [flags]

Flags:
  -h, --help               help for wait
      --request-id         interpret the id as the client idempotency key
      --timeout duration   give up after this long (default 1m0s)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest portfolio

```
Portfolio totals and holdings

Usage:
  tinvest portfolio [command]

Available Commands:
  get         Get portfolio totals, yield, and positions

Flags:
  -h, --help   help for portfolio

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest portfolio [command] --help" for more information about a command.
```

### tinvest portfolio get

```
Get portfolio totals, yield, and positions

Usage:
  tinvest portfolio get [flags]

Flags:
  -h, --help   help for get

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest positions

```
Account positions and blocked quantities

Usage:
  tinvest positions [command]

Available Commands:
  get         Get money, securities, futures, and options positions

Flags:
  -h, --help   help for positions

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest positions [command] --help" for more information about a command.
```

### tinvest positions get

```
Get money, securities, futures, and options positions

Usage:
  tinvest positions get [flags]

Flags:
  -h, --help   help for get

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest quotes

```
Market quotes

Usage:
  tinvest quotes [command]

Available Commands:
  close       Trading-session close price for one or more instruments
  last        Last trade price for one or more instruments

Flags:
  -h, --help   help for quotes

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest quotes [command] --help" for more information about a command.
```

### tinvest quotes close

```
Trading-session close price for one or more instruments

Usage:
  tinvest quotes close <id...> [flags]

Flags:
  -h, --help       help for close
      --no-cache   bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest quotes last

```
Last trade price for one or more instruments

Usage:
  tinvest quotes last <id...> [flags]

Flags:
  -h, --help       help for last
      --no-cache   bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest research

```
News, fundamentals, forecasts, and insider activity

Usage:
  tinvest research [command]

Available Commands:
  consensus     Get one page of instrument consensus forecasts
  forecast      Get investment-house forecasts for one instrument
  fundamentals  Get fundamentals for asset UIDs or resolved instruments
  insider-deals Get one page of insider deals for one instrument
  news          Get one page of current news

Flags:
  -h, --help   help for research

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest research [command] --help" for more information about a command.
```

### tinvest research consensus

```
Get one page of instrument consensus forecasts

Usage:
  tinvest research consensus [flags]

Flags:
  -h, --help                help for consensus
      --limit int32         consensus forecasts per page (default 100)
      --page-number int32   zero-based page number

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest research forecast

```
Get investment-house forecasts for one instrument

Usage:
  tinvest research forecast [flags]

Flags:
  -h, --help                help for forecast
      --instrument string   instrument id: UID, FIGI, or TICKER@CLASSCODE (required)
      --no-cache            bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest research fundamentals

```
Get fundamentals for asset UIDs or resolved instruments

Usage:
  tinvest research fundamentals [flags]

Flags:
      --asset stringArray        asset UID (repeatable)
  -h, --help                     help for fundamentals
      --instrument stringArray   instrument id resolved to asset UID (repeatable: UID, FIGI, or TICKER@CLASSCODE)
      --no-cache                 bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest research insider-deals

```
Get one page of insider deals for one instrument

Usage:
  tinvest research insider-deals [flags]

Flags:
      --cursor string       cursor from a previous response
  -h, --help                help for insider-deals
      --instrument string   instrument id: UID, FIGI, or TICKER@CLASSCODE (required)
      --limit int32         insider deals per page (1 through 100) (default 100)
      --no-cache            bypass the local instrument cache

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest research news

```
Get one page of current news

Usage:
  tinvest research news [flags]

Flags:
      --cursor int    cursor from a previous response
  -h, --help          help for news
      --limit int32   news items per page (default 1000)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest sandbox

```
Manage sandbox accounts (always targets the sandbox endpoint)

Usage:
  tinvest sandbox [command]

Available Commands:
  accounts    List sandbox accounts visible to the token
  close       Close a sandbox account
  open        Open a new sandbox account
  topup       Credit virtual money to a sandbox account (SandboxPayIn)

Flags:
  -h, --help   help for sandbox

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest sandbox [command] --help" for more information about a command.
```

### tinvest sandbox accounts

```
List sandbox accounts visible to the token

Usage:
  tinvest sandbox accounts [flags]

Flags:
  -h, --help   help for accounts

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest sandbox close

```
Close a sandbox account

Usage:
  tinvest sandbox close <account-id> [flags]

Flags:
  -h, --help   help for close

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest sandbox open

```
Open a new sandbox account

Usage:
  tinvest sandbox open [flags]

Flags:
  -h, --help          help for open
      --name string   optional account name

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest sandbox topup

```
Credit virtual money to a sandbox account (SandboxPayIn)

Usage:
  tinvest sandbox topup [flags]

Flags:
      --amount string     amount to credit as a decimal string (required)
      --currency string   currency code (the API documents this as a ruble top-up) (default "rub")
  -h, --help              help for topup

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest signals

```
Analyst and technical signals

Usage:
  tinvest signals [command]

Available Commands:
  list        List signals, optionally filtered by strategy
  strategies  List signal strategies

Flags:
  -h, --help   help for signals

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest signals [command] --help" for more information about a command.
```

### tinvest signals list

```
List signals, optionally filtered by strategy

Usage:
  tinvest signals list [flags]

Flags:
  -h, --help              help for list
      --strategy string   strategy id filter

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest signals strategies

```
List signal strategies

Usage:
  tinvest signals strategies [flags]

Flags:
  -h, --help   help for strategies

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest stop-orders

```
Place, list, cancel, and reconcile stop orders (take-profit, stop-loss, stop-limit)

Usage:
  tinvest stop-orders [command]

Available Commands:
  cancel      Cancel an active stop order (idempotent)
  list        List stop orders on the account
  place       Place a stop order (idempotent, journaled; never auto-retried)
  reconcile   Resolve every unconfirmed stop-order intent in the journal against the broker

Flags:
  -h, --help   help for stop-orders

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest stop-orders [command] --help" for more information about a command.
```

### tinvest stop-orders cancel

```
Cancel an active stop order (idempotent)

Usage:
  tinvest stop-orders cancel <stop-order-id> [flags]

Flags:
  -h, --help   help for cancel

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest stop-orders list

```
List stop orders on the account

Usage:
  tinvest stop-orders list [flags]

Flags:
  -h, --help            help for list
      --status string   filter: all, active, executed, canceled, or expired (default: broker's own default)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest stop-orders place

```
Place a stop order (idempotent, journaled; never auto-retried)

Usage:
  tinvest stop-orders place [flags]

Flags:
      --direction string              buy or sell
      --dry-run                       validate only, no network call (stop orders have no GetOrderPrice/GetMaxLots equivalent)
      --exchange-order-type string    market or limit: the child order type for take-profit orders (default market)
      --expiration string             gtc or gtd (default "gtc")
      --expire-date string            RFC3339 timestamp; required when --expiration gtd
  -h, --help                          help for place
      --input string                  read the full request as JSON from a file or - for stdin
      --instrument string             instrument id: uid, FIGI, or TICKER@CLASSCODE
      --no-cache                      bypass the local instrument cache
      --order-id string               client idempotency key (UUID); generated if omitted
      --price string                  limit price as a decimal string (required for stop-limit only)
      --quantity int                  number of lots (positive)
      --stop-price string             stop-activation price as a decimal string (required)
      --take-profit-type string       regular or trailing (take-profit only; default regular for take-profit)
      --trailing-indent string        trailing take-profit indent, decimal string
      --trailing-indent-type string   absolute or relative
      --trailing-spread string        trailing take-profit protective spread, decimal string
      --trailing-spread-type string   absolute or relative
      --type string                   take-profit, stop-loss, or stop-limit
      --yes                           confirm the mutation (accepted for symmetry; no interactive prompt)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest stop-orders reconcile

```
Resolve every unconfirmed stop-order intent in the journal against the broker

Usage:
  tinvest stop-orders reconcile [flags]

Flags:
  -h, --help   help for reconcile

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest stream

```
Resilient broker streams as NDJSON

Usage:
  tinvest stream [command]

Available Commands:
  marketdata  Stream candles, order books, trades, last prices, and trading status
  orders      Stream executed order trades
  portfolio   Stream portfolio snapshots and updates
  positions   Stream initial positions and balance changes

Flags:
  -h, --help   help for stream

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest stream [command] --help" for more information about a command.
```

### tinvest stream marketdata

```
Stream candles, order books, trades, last prices, and trading status

Usage:
  tinvest stream marketdata [flags]

Flags:
      --candles string[="1m"]    stream candles at interval 1m..1M (default 1m when omitted)
  -h, --help                     help for marketdata
      --info                     stream trading status
      --instrument stringArray   instrument id (repeatable: UID, FIGI, or TICKER@CLASSCODE)
      --last-price               stream last prices
      --orderbook int32[=20]     stream order book at depth 1, 10, 20, 30, 40, or 50 (default 20 when omitted)
      --trades                   stream public trades

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest stream orders

```
Stream executed order trades

Usage:
  tinvest stream orders [flags]

Flags:
  -h, --help   help for orders

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest stream portfolio

```
Stream portfolio snapshots and updates

Usage:
  tinvest stream portfolio [flags]

Flags:
  -h, --help   help for portfolio

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest stream positions

```
Stream initial positions and balance changes

Usage:
  tinvest stream positions [flags]

Flags:
  -h, --help   help for positions

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest token

```
API token utilities

Usage:
  tinvest token [command]

Available Commands:
  check       Validate the token and report its access

Flags:
  -h, --help   help for token

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest token [command] --help" for more information about a command.
```

### tinvest token check

```
Calls GetInfo and GetAccounts with the resolved token and reports the user
profile, the visible accounts with their access levels, and hints about what
kind of token this looks like. Exits 3 when the broker rejects the token.

Usage:
  tinvest token check [flags]

Flags:
  -h, --help   help for check

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest trades

```
Executed trades from operation history

Usage:
  tinvest trades [command]

Available Commands:
  list        List executions nested under executed operations

Flags:
  -h, --help   help for trades

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest trades [command] --help" for more information about a command.
```

### tinvest trades list

```
List executions nested under executed operations

Usage:
  tinvest trades list [flags]

Flags:
      --all                 follow cursors and return all pages
      --cursor string       cursor from a previous response
      --from string         period start as RFC3339 (optional)
  -h, --help                help for list
      --instrument string   instrument id filter: uid, FIGI, or TICKER@CLASSCODE
      --limit int32         operations per page (3 through 1000) (default 100)
      --to string           period end as RFC3339 (optional)

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest user

```
User tariff and account attributes

Usage:
  tinvest user [command]

Available Commands:
  margin      Get margin attributes for an account
  tariff      Get unary and stream limits for the current tariff

Flags:
  -h, --help   help for user

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)

Use "tinvest user [command] --help" for more information about a command.
```

### tinvest user margin

```
Get margin attributes for an account

Usage:
  tinvest user margin [flags]

Flags:
  -h, --help   help for margin

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

### tinvest user tariff

```
Get unary and stream limits for the current tariff

Usage:
  tinvest user tariff [flags]

Flags:
  -h, --help   help for tariff

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```

## tinvest version

```
Print CLI, contract, and schema versions

Usage:
  tinvest version [flags]

Flags:
  -h, --help   help for version

Global Flags:
      --account string      account id for account-scoped commands
      --no-rate-limit       disable client-side unary rate limiting
  -o, --output string       output format: json or table (env TINVEST_OUTPUT)
      --profile string      config profile name (env TINVEST_PROFILE)
      --sandbox             shortcut: use the sandbox endpoint
      --timeout duration    per-call deadline (default 10s)
      --token-file string   file containing the API token (overrides TINVEST_TOKEN)
```
