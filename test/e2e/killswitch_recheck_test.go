package e2e

import (
	"os"
	"path/filepath"
	"testing"
)

// TestKillSwitchEngagedDuringResolveBlocksSend is the F11(b) regression: the
// kill switch is absent when the initial policy gate runs, then the operator
// engages it while the CLI is resolving the instrument. The re-check immediately
// before the send must catch it — the order must NOT go out. On 4304f5a (no
// re-check) the placement proceeds to PostOrder despite the switch.
func TestKillSwitchEngagedDuringResolveBlocksSend(t *testing.T) {
	f := newFakeServer(t)
	h := newHarness(t, f.endpoint())

	killFile := filepath.Join(h.root, "killswitch")
	policyFile := filepath.Join(h.root, "policy.toml")
	if err := os.WriteFile(policyFile, []byte("kill_switch_file = \""+killFile+"\"\n"), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	h.writeConfig("policy_file = \"" + policyFile + "\"\n")

	// The switch does not exist yet — the initial gate passes. Engage it during
	// instrument resolution, i.e. after that gate but before the send.
	f.setOnGetInstrument(func() {
		_ = os.WriteFile(killFile, []byte("engaged"), 0o600)
	})

	orderID := "77777777-7777-4777-8777-777777777777"
	res := h.run(placeArgs(orderID)...)

	if res.exit != 2 {
		t.Fatalf("exit = %d, want 2 (kill switch engaged mid-flight)\nstdout: %s\nstderr: %s", res.exit, res.stdout, res.stderr)
	}
	env := decodeEnvelope(t, res.stdout)
	if env.OK || env.Error == nil || env.Error.Code != "POLICY" {
		t.Errorf("error.code = %v, want POLICY: %s", env.Error, res.stdout)
	}
	// The instrument was resolved (proving we got past the initial gate)...
	if n := f.instrLookupCount(); n != 1 {
		t.Errorf("instrument lookups = %d, want 1", n)
	}
	// ...but the order must NOT have been sent.
	if n := len(f.postOrderRequests()); n != 0 {
		t.Errorf("fake saw %d PostOrder calls, want 0 — the re-check must block the send", n)
	}
}
