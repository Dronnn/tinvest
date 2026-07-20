//go:build e2elive

package e2elive

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Dronnn/tinvest/internal/config"
)

// requireLive skips a test cleanly when no sandbox token is configured, so a
// tagged run without credentials passes rather than fails.
func requireLive(t *testing.T) {
	t.Helper()
	if !tokenPresent() {
		t.Skip("live sandbox suite: set TINVEST_TOKEN to run")
	}
}

// newOrderID returns a fresh random UUIDv4 client order id. Randomizing per run
// keeps idempotency-key lookups unambiguous and immune to state left by a prior
// run on a since-swept account.
func newOrderID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// harness owns one hermetic filesystem sandbox: an isolated XDG config/state/
// cache tree plus helpers to launch the binary, either DIRECT against the real
// sandbox (--sandbox, so the endpoint is structurally the sandbox host) or
// through a loopback relay (a profile pinned to 127.0.0.1). The journal lives
// under stateDir, isolated from the developer's real journal.
type harness struct {
	t        *testing.T
	root     string
	cfgDir   string
	stateDir string
	cacheDir string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	root := t.TempDir()
	h := &harness{
		t:        t,
		root:     root,
		cfgDir:   filepath.Join(root, "config"),
		stateDir: filepath.Join(root, "state"),
		cacheDir: filepath.Join(root, "cache"),
	}
	for _, d := range []string{h.cfgDir, h.stateDir, h.cacheDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	return h
}

// baseEnv is the fully-isolated environment for a subprocess: config, state, and
// cache under the harness root, the live token (never logged), and the given CA
// bundle. caFile empty leaves TINVEST_CA_FILE unset (system trust store).
func (h *harness) baseEnv(caFile string) []string {
	env := []string{
		"HOME=" + h.root,
		"XDG_CONFIG_HOME=" + h.cfgDir,
		"XDG_STATE_HOME=" + h.stateDir,
		"XDG_CACHE_HOME=" + h.cacheDir,
		config.EnvToken + "=" + liveToken,
		"PATH=" + os.Getenv("PATH"),
	}
	if caFile != "" {
		env = append(env, config.EnvCAFile+"="+caFile)
	}
	return env
}

// writeRelayConfig points the default profile at a loopback relay. The address
// is asserted to be loopback so a relay run can never be aimed at a real host.
func (h *harness) writeRelayConfig(relayAddr string) {
	h.t.Helper()
	assertLoopback(h.t, relayAddr)
	dir := filepath.Join(h.cfgDir, "tinvest")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		h.t.Fatalf("mkdir config: %v", err)
	}
	content := "default_profile = \"sblive\"\n\n[profiles.sblive]\nendpoint = \"" + relayAddr + "\"\n"
	if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte(content), 0o600); err != nil {
		h.t.Fatalf("write config: %v", err)
	}
}

// runDirect runs the binary DIRECT against the sandbox. It always injects
// --sandbox, which makes config.Load resolve the endpoint to config.Sandbox-
// Endpoint regardless of any profile — the structural guarantee that a direct
// invocation cannot address production.
func (h *harness) runDirect(args ...string) cliResult {
	h.t.Helper()
	full := append([]string{"--sandbox"}, args...)
	return h.exec(h.baseEnv(directCAFile), full)
}

// runRelay runs the binary through the relay (default profile -> loopback). No
// --sandbox: the profile endpoint is used, which writeRelayConfig has pinned to
// 127.0.0.1. The relay itself only ever dials config.SandboxEndpoint upstream.
func (h *harness) runRelay(args ...string) cliResult {
	h.t.Helper()
	return h.exec(h.baseEnv(runtimeCAPath), args)
}

// relayCommand builds an un-started relay invocation so a test can Start it and
// SIGKILL it at a precise moment.
func (h *harness) relayCommand(args ...string) *exec.Cmd {
	cmd := exec.Command(binaryPath, args...)
	cmd.Env = h.baseEnv(runtimeCAPath)
	return cmd
}

func (h *harness) exec(env []string, args []string) cliResult {
	h.t.Helper()
	cmd := exec.Command(binaryPath, args...)
	cmd.Env = env
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	return cliResult{exit: exitCode(err), stdout: out.String(), stderr: errb.String()}
}

// cliResult is one finished invocation's observable output.
type cliResult struct {
	exit   int
	stdout string
	stderr string
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1 // failed to start or non-exit failure
}

// --- safety guards ---

// assertLoopback fails immediately unless addr is a loopback address. Combined
// with the relay's hardwired sandbox upstream, this makes it impossible for a
// relay run to touch any non-sandbox host.
func assertLoopback(t *testing.T, addr string) {
	t.Helper()
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatalf("refusing to run relay against non-loopback address %q", addr)
	}
}

// --- envelope + stdout discipline ---

type envelope struct {
	OK    bool `json:"ok"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Phase   string `json:"phase"`
	} `json:"error"`
	Data json.RawMessage `json:"data"`
	Meta struct {
		AccountID     string `json:"account_id"`
		TrackingID    string `json:"tracking_id"`
		Contract      string `json:"contract"`
		SchemaVersion string `json:"schema_version"`
	} `json:"meta"`
}

// assertCleanStdout enforces the stdout contract: stdout is either empty or
// exactly one JSON value, never log noise and never a partial or double write.
func assertCleanStdout(t *testing.T, stdout string) {
	t.Helper()
	if strings.TrimSpace(stdout) == "" {
		return
	}
	dec := json.NewDecoder(strings.NewReader(stdout))
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Errorf("stdout not decodable as a single JSON value: %v (stdout=%q)", err, stdout)
		return
	}
	if dec.More() {
		t.Errorf("stdout carries more than one JSON value: %q", stdout)
	}
}

// decodeEnvelope parses a JSON envelope off stdout, failing if stdout is not a
// single well-formed envelope.
func decodeEnvelope(t *testing.T, stdout string) envelope {
	t.Helper()
	assertCleanStdout(t, stdout)
	var env envelope
	if err := json.Unmarshal([]byte(stdout), &env); err != nil {
		t.Fatalf("decode envelope: %v (stdout=%q)", err, stdout)
	}
	if env.Meta.SchemaVersion != "0.1" {
		t.Errorf("meta.schema_version = %q, want 0.1", env.Meta.SchemaVersion)
	}
	return env
}

// assertOK requires a clean success: exit 0, ok:true, and (for a single broker
// round-trip) a broker tracking id in meta. Reconcile aggregates several calls
// and reports no single tracking id, so callers pass wantTracking=false there.
func (h *harness) assertOK(res cliResult, wantTracking bool) envelope {
	h.t.Helper()
	if res.exit != 0 {
		h.t.Fatalf("exit = %d, want 0\nstdout: %s\nstderr: %s", res.exit, res.stdout, res.stderr)
	}
	env := decodeEnvelope(h.t, res.stdout)
	if !env.OK {
		h.t.Fatalf("envelope ok = false, want true: %s", res.stdout)
	}
	if wantTracking && env.Meta.TrackingID == "" {
		h.t.Errorf("meta.tracking_id is empty on a broker round-trip: %s", res.stdout)
	}
	return env
}

// --- journal (write-ahead ledger) reading ---

type ledgerRecord struct {
	Seq      int64  `json:"seq"`
	Stage    string `json:"stage"`
	IntentID string `json:"intent_id"`
	OrderID  string `json:"order_id"`
	Kind     string `json:"kind"`
}

// ledger reads and parses every journal line the CLI wrote, ordered by seq.
func (h *harness) ledger() []ledgerRecord {
	h.t.Helper()
	dir := filepath.Join(h.stateDir, "tinvest", "journal")
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		h.t.Fatalf("glob journal: %v", err)
	}
	var recs []ledgerRecord
	for _, fp := range files {
		data, err := os.ReadFile(fp)
		if err != nil {
			h.t.Fatalf("read journal %s: %v", fp, err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var r ledgerRecord
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				h.t.Fatalf("parse journal line %q: %v", line, err)
			}
			recs = append(recs, r)
		}
	}
	sort.SliceStable(recs, func(i, j int) bool { return recs[i].Seq < recs[j].Seq })
	return recs
}

func stagesFor(recs []ledgerRecord, intentID string) []string {
	var out []string
	for _, r := range recs {
		if r.IntentID == intentID {
			out = append(out, r.Stage)
		}
	}
	return out
}

func hasStageFor(recs []ledgerRecord, intentID, stage string) bool {
	for _, r := range recs {
		if r.IntentID == intentID && r.Stage == stage {
			return true
		}
	}
	return false
}
