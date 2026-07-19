# Live sandbox integration suite

Runs the compiled `tinvest` binary as a subprocess against the **real T-Invest
sandbox** (`sandbox-invest-public-api.tbank.ru`). It is opt-in: every file
carries the `e2elive` build tag, so a plain `go test ./...` never compiles it,
and each test `t.Skip`s cleanly when `TINVEST_TOKEN` is unset.

Run it with the sandbox token exported (and the Russian CA bundle, needed to
reach T-Bank's endpoints):

    TINVEST_TOKEN=… TINVEST_CA_FILE=~/.config/tinvest/russian-trusted-ca.pem \
        go test -tags e2elive -race ./test/e2e-live/...

## What it covers

- **`TestSandboxLifecycle`** — a full scripted order lifecycle, all direct
  against the sandbox: open → topup → place (limit) → get → list → replace →
  cancel → stop-orders place/list/cancel → reconcile → close. Every step asserts
  the exit code and the JSON envelope (`ok:true`, `meta.tracking_id` on broker
  round-trips). MOEX is closed at weekends, so orders rest as accepted (`new`);
  no fills are asserted.
- **`TestKillAfterSendThenReconcileLive`** — crash injection against the real
  sandbox. A transparent loopback gRPC relay forwards placement to the sandbox
  and signals the instant the broker's response arrives, so the CLI is SIGKILLed
  after `send-started` is journaled and the request is on the wire, but before
  any confirmation is recorded. `orders reconcile` then converges the intent to
  the sandbox's truth, and a direct sandbox read confirms exactly one order
  exists for the idempotency key.

## Safety

The suite is structurally incapable of reaching production. Direct invocations
always pass `--sandbox` (which pins the endpoint to the sandbox host regardless
of any profile); the relay serves a loopback address (asserted) and its upstream
is hardwired to the sandbox endpoint constant (asserted not to equal the prod
host). The token reaches the binary only through the environment and is never
logged; the relay forwards the incoming authorization metadata upstream without
ever handling it as a string.

Sandbox accounts are disposable: each test sweeps every existing sandbox account
closed before it runs, and `t.Cleanup` cancels outstanding orders and closes any
account it opened — so the suite is safe to re-run back to back.
