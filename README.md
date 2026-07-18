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
tinvest version         # CLI version + pinned contract version
```

Planned command groups: `accounts`, `portfolio`, `positions`, `balance`, `instruments`, `quotes`, `orderbook`, `candles`, `orders`, `stop-orders`, `operations`, `stream`, `sandbox`, `token`.

## Disclaimer

This is an independent open-source project, not affiliated with or endorsed by T-Bank. Trading involves risk; you are solely responsible for any operations executed through this tool. Use a sandbox or read-only token whenever possible.
