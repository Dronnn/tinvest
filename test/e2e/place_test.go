package e2e

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	investapi "tinvest/internal/pb/investapi"
)

// TestPlaceHappyPath: a clean placement exits 0, emits a single ok envelope,
// and journals intent-created -> send-started -> broker-confirmed (plan §9/§10).
func TestPlaceHappyPath(t *testing.T) {
	f := newFakeServer(t)
	h := newHarness(t, f.endpoint())
	h.writeConfig("")

	orderID := "22222222-2222-4222-8222-222222222222"
	res := h.run(placeArgs(orderID)...)

	if res.exit != 0 {
		t.Fatalf("exit = %d, want 0\nstdout: %s\nstderr: %s", res.exit, res.stdout, res.stderr)
	}
	env := decodeEnvelope(t, res.stdout)
	if !env.OK {
		t.Errorf("envelope ok = false, want true: %s", res.stdout)
	}

	got := stagesFor(h.ledger(), orderID)
	want := []string{"intent-created", "send-started", "broker-confirmed"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ledger stages = %v, want %v", got, want)
	}

	posts := f.postOrderRequests()
	if len(posts) != 1 {
		t.Fatalf("fake saw %d PostOrder calls, want 1", len(posts))
	}
	if posts[0].GetOrderId() != orderID {
		t.Errorf("PostOrder order_id = %q, want %q", posts[0].GetOrderId(), orderID)
	}
}

// TestPlaceBrokerRejection: a definitive INVALID_ARGUMENT from the broker is
// exit 5 with a BROKER_REJECTED envelope and a broker-rejected ledger stage —
// and no broker-confirmed (plan §7/§9).
func TestPlaceBrokerRejection(t *testing.T) {
	f := newFakeServer(t)
	f.onPostOrder = func(context.Context, *investapi.PostOrderRequest) (*investapi.PostOrderResponse, error) {
		return nil, invalidArgument("30042") // broker numeric error code
	}
	h := newHarness(t, f.endpoint())
	h.writeConfig("")

	orderID := "44444444-4444-4444-8444-444444444444"
	res := h.run(placeArgs(orderID)...)

	if res.exit != 5 {
		t.Fatalf("exit = %d, want 5\nstdout: %s\nstderr: %s", res.exit, res.stdout, res.stderr)
	}
	env := decodeEnvelope(t, res.stdout)
	if env.OK {
		t.Errorf("envelope ok = true, want false: %s", res.stdout)
	}
	if env.Error == nil || env.Error.Code != "BROKER_REJECTED" {
		t.Errorf("error.code = %v, want BROKER_REJECTED: %s", env.Error, res.stdout)
	}

	recs := h.ledger()
	if !hasStage(recs, "broker-rejected") {
		t.Errorf("ledger missing broker-rejected stage: %v", stagesFor(recs, orderID))
	}
	if hasStage(recs, "broker-confirmed") {
		t.Errorf("ledger has broker-confirmed on a rejection: %v", stagesFor(recs, orderID))
	}
}

// TestKillSwitchBlocksPlacement: a kill-switch file present makes placement fail
// locally with exit 2 / POLICY before any network call, and before any ledger
// entry is created (plan §6/§9).
func TestKillSwitchBlocksPlacement(t *testing.T) {
	f := newFakeServer(t)
	h := newHarness(t, f.endpoint())

	killFile := filepath.Join(h.root, "killswitch")
	if err := os.WriteFile(killFile, []byte("engaged"), 0o600); err != nil {
		t.Fatalf("write kill switch: %v", err)
	}
	policyFile := filepath.Join(h.root, "policy.toml")
	if err := os.WriteFile(policyFile, []byte("kill_switch_file = \""+killFile+"\"\n"), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	h.writeConfig("policy_file = \"" + policyFile + "\"\n")

	orderID := "55555555-5555-4555-8555-555555555555"
	res := h.run(placeArgs(orderID)...)

	if res.exit != 2 {
		t.Fatalf("exit = %d, want 2\nstdout: %s\nstderr: %s", res.exit, res.stdout, res.stderr)
	}
	env := decodeEnvelope(t, res.stdout)
	if env.OK {
		t.Errorf("envelope ok = true, want false: %s", res.stdout)
	}
	if env.Error == nil || env.Error.Code != "POLICY" {
		t.Errorf("error.code = %v, want POLICY: %s", env.Error, res.stdout)
	}

	if recs := h.ledger(); len(recs) != 0 {
		t.Errorf("kill switch wrote %d ledger records, want 0: %+v", len(recs), recs)
	}
	if n := len(f.postOrderRequests()); n != 0 {
		t.Errorf("fake saw %d PostOrder calls, want 0", n)
	}
	if n := f.instrLookupCount(); n != 0 {
		t.Errorf("fake saw %d instrument lookups, want 0 (kill switch precedes connect)", n)
	}
}

// TestConcurrentDuplicateIntents: two simultaneous places with the SAME
// --order-id both succeed. Key stability is the CLI's job (identical order_id on
// both wire calls); broker-side dedup is the broker's. The shared journal ends
// up with two distinct intent entries carrying that one order_id.
func TestConcurrentDuplicateIntents(t *testing.T) {
	f := newFakeServer(t)
	h := newHarness(t, f.endpoint())
	h.writeConfig("")

	orderID := "66666666-6666-4666-8666-666666666666"
	var wg sync.WaitGroup
	results := make([]cliResult, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = h.run(placeArgs(orderID)...)
		}(i)
	}
	wg.Wait()

	for i, res := range results {
		if res.exit != 0 {
			t.Fatalf("run %d exit = %d, want 0\nstdout: %s\nstderr: %s", i, res.exit, res.stdout, res.stderr)
		}
		env := decodeEnvelope(t, res.stdout)
		if !env.OK {
			t.Errorf("run %d envelope ok = false: %s", i, res.stdout)
		}
	}

	posts := f.postOrderRequests()
	if len(posts) != 2 {
		t.Fatalf("fake saw %d PostOrder calls, want 2", len(posts))
	}
	for i, p := range posts {
		if p.GetOrderId() != orderID {
			t.Errorf("PostOrder %d order_id = %q, want %q (key stability)", i, p.GetOrderId(), orderID)
		}
	}

	if n := countStageWithOrderID(h.ledger(), "intent-created", orderID); n != 2 {
		t.Errorf("journal has %d intent-created entries for order_id %s, want 2", n, orderID)
	}
}
