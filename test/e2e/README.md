# End-to-end crash-injection suite

Runs the compiled `tinvest` binary as a subprocess against an in-process TLS
gRPC fake and asserts the ledger stages, exit codes, and stdout/stderr
discipline of the placement and reconcile flows (plan §11.2).

Run it with:

    go test ./test/e2e/...
