package e2e

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// harness owns one hermetic sandbox: an isolated XDG config/state/cache tree
// pointing the CLI at a fake endpoint, plus helpers to launch the binary and
// read back the intent journal it wrote.
type harness struct {
	t        *testing.T
	root     string
	cfgDir   string
	stateDir string
	cacheDir string
	endpoint string
}

func newHarness(t *testing.T, endpoint string) *harness {
	t.Helper()
	root := t.TempDir()
	h := &harness{
		t:        t,
		root:     root,
		cfgDir:   filepath.Join(root, "config"),
		stateDir: filepath.Join(root, "state"),
		cacheDir: filepath.Join(root, "cache"),
		endpoint: endpoint,
	}
	for _, d := range []string{h.cfgDir, h.stateDir, h.cacheDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	return h
}

// writeConfig writes config.toml with a single default profile pointing at the
// fake. extraProfileLines is appended verbatim inside the profile table (used
// to attach a policy_file for the kill-switch test).
func (h *harness) writeConfig(extraProfileLines string) {
	h.t.Helper()
	dir := filepath.Join(h.cfgDir, "tinvest")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		h.t.Fatalf("mkdir config: %v", err)
	}
	content := "default_profile = \"test\"\n\n[profiles.test]\nendpoint = \"" + h.endpoint + "\"\n" + extraProfileLines
	if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte(content), 0o600); err != nil {
		h.t.Fatalf("write config: %v", err)
	}
}

// env is the fully-isolated environment for a subprocess: config, state, and
// cache all under the harness temp root, a dummy token, and the test CA.
func (h *harness) env() []string {
	return []string{
		"HOME=" + h.root,
		"XDG_CONFIG_HOME=" + h.cfgDir,
		"XDG_STATE_HOME=" + h.stateDir,
		"XDG_CACHE_HOME=" + h.cacheDir,
		"TINVEST_TOKEN=dummy-token",
		"TINVEST_CA_FILE=" + caCertPath,
		"PATH=" + os.Getenv("PATH"),
	}
}

func (h *harness) command(args ...string) *exec.Cmd {
	cmd := exec.Command(binaryPath, args...)
	cmd.Env = h.env()
	return cmd
}

// cliResult is one finished invocation's observable output.
type cliResult struct {
	exit   int
	stdout string
	stderr string
}

func (h *harness) run(args ...string) cliResult {
	h.t.Helper()
	cmd := h.command(args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	return cliResult{exit: exitCode(err), stdout: out.String(), stderr: errb.String()}
}

// placeArgs builds a canonical `orders place` invocation. --no-cache keeps
// resolution deterministic (every run hits the fake's GetInstrumentBy) and
// removes any cross-run cache coupling in the concurrent test.
func placeArgs(orderID string) []string {
	return []string{
		"orders", "place",
		"--instrument", testUID,
		"--direction", "buy",
		"--quantity", "1",
		"--type", "limit",
		"--price", "100",
		"--account", "acc-1",
		"--order-id", orderID,
		"--yes",
		"--no-cache",
		"-o", "json",
	}
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

// --- ledger reading ---

// ledgerRecord is the subset of a journal line the suite asserts on.
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

func hasStage(recs []ledgerRecord, stage string) bool {
	for _, r := range recs {
		if r.Stage == stage {
			return true
		}
	}
	return false
}

func countStageWithOrderID(recs []ledgerRecord, stage, orderID string) int {
	n := 0
	for _, r := range recs {
		if r.Stage == stage && r.OrderID == orderID {
			n++
		}
	}
	return n
}

func recordWithStage(recs []ledgerRecord, stage string) (ledgerRecord, bool) {
	for _, r := range recs {
		if r.Stage == stage {
			return r, true
		}
	}
	return ledgerRecord{}, false
}

// --- envelope + stdout discipline ---

type envelope struct {
	OK    bool `json:"ok"`
	Error *struct {
		Code string `json:"code"`
	} `json:"error"`
}

// assertCleanStdout enforces the stdout contract (plan §7, task 2e): stdout is
// either empty or exactly one JSON value, never log noise and never a partial
// or double write.
func assertCleanStdout(t *testing.T, stdout string) {
	t.Helper()
	if strings.TrimSpace(stdout) == "" {
		return
	}
	if !json.Valid([]byte(stdout)) {
		t.Errorf("stdout is not valid JSON: %q", stdout)
		return
	}
	dec := json.NewDecoder(strings.NewReader(stdout))
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Errorf("stdout not decodable as a single JSON value: %v", err)
		return
	}
	if dec.More() {
		t.Errorf("stdout carries more than one JSON value: %q", stdout)
	}
}

// decodeEnvelope parses a JSON envelope off stdout and fails the test if stdout
// is not a single well-formed envelope.
func decodeEnvelope(t *testing.T, stdout string) envelope {
	t.Helper()
	assertCleanStdout(t, stdout)
	var env envelope
	if err := json.Unmarshal([]byte(stdout), &env); err != nil {
		t.Fatalf("decode envelope: %v (stdout=%q)", err, stdout)
	}
	return env
}

// --- gRPC status helpers for fake hooks ---

func notFound() error { return status.Error(codes.NotFound, "order not found") }

func invalidArgument(msg string) error { return status.Error(codes.InvalidArgument, msg) }
