# Vendored T-Invest API contracts

These `.proto` files are vendored verbatim from T-Bank's contract repository.
Do not edit them by hand. The generated Go stubs live in
`pb/investapi/` and are regenerated with `make proto`.

## Pinned source

| Field | Value |
|---|---|
| Upstream repo | https://opensource.tbank.ru/invest/invest-contracts |
| Contract release | **1.49** |
| Git ref (tag) | `1.49` |
| Commit SHA | `ef3337c71b7d6dffe61dfdef814fc4e603004f8b` |
| Release commit date | 2026-05-22 |
| Fetched on | 2026-07-18 |
| Source path in repo | `src/docs/contracts/` |

Fetched anonymously via the GitLab REST API, e.g.:

```
curl "https://opensource.tbank.ru/api/v4/projects/invest%2Finvest-contracts/repository/files/src%2Fdocs%2Fcontracts%2Forders.proto/raw?ref=1.49"
```

## Files

| File | Notes |
|---|---|
| `common.proto` | Shared types: `MoneyValue`, `Quotation`, enums |
| `instruments.proto` | InstrumentsService |
| `marketdata.proto` | MarketDataService + MarketDataStreamService |
| `operations.proto` | OperationsService + OperationsStreamService |
| `orders.proto` | OrdersService + OrdersStreamService |
| `stoporders.proto` | StopOrdersService |
| `sandbox.proto` | SandboxService |
| `users.proto` | UsersService |
| `signals.proto` | SignalService |
| `google/api/field_behavior.proto` | Vendored dependency imported by the contracts |

The upstream `option go_package` is `"./;investapi"`. Codegen (buf managed
mode, see `buf.gen.yaml`) overrides it to `github.com/Dronnn/tinvest/pb/investapi` with
Go package name `investapi`; `google/api/field_behavior.proto` is folded into
that same package so the tree has no external annotation dependency.

## Contract-drift check

To compare the vendored contracts against upstream HEAD without refetching:

```
make proto-lint
# or a breaking-change diff against a newer ref, once vendored anew.
```
