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
```

Token resolution order: `--token-file` flag, then `TINVEST_TOKEN`, then the profile's `token_file`.

Planned command groups: `portfolio`, `positions`, `balance`, `instruments`, `quotes`, `orderbook`, `candles`, `orders`, `stop-orders`, `operations`, `stream`, `sandbox`.

## Disclaimer

This is an independent open-source project, not affiliated with or endorsed by T-Bank. Trading involves risk; you are solely responsible for any operations executed through this tool. Use a sandbox or read-only token whenever possible.
