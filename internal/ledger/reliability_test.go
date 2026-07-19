package ledger

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func beginIntent(t *testing.T, l *Ledger, intentID, orderID string) {
	t.Helper()
	if _, err := l.Begin(Intent{
		IntentID: intentID, Kind: "order.place", AccountID: "acc", Profile: "sandbox",
		Attempt: 1, OrderID: orderID, Payload: samplePayload{AccountID: "acc", OrderID: orderID},
	}); err != nil {
		t.Fatalf("Begin %s: %v", intentID, err)
	}
}

func hasID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func currentMonthPath(dir string) string {
	return filepath.Join(dir, time.Now().UTC().Format(monthFmt)+".jsonl")
}

// TestTornTailRepairPreservesNextAppend is the F7 regression: after a torn final
// write (a partial line at EOF), reopening the ledger repairs the tail so a
// subsequent append lands on a clean record boundary and is recoverable — rather
// than being concatenated onto the torn bytes and lost. Uses only the pre-fix
// API, so it also fails on eabce2e (where intent-3 vanished).
func TestTornTailRepairPreservesNextAppend(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	beginIntent(t, l, "intent-1", "ord-1")
	beginIntent(t, l, "intent-2", "ord-2")
	_ = l.Close()

	// Simulate a torn final write: drop the trailing newline and part of the
	// last record so the file ends mid-line.
	path := currentMonthPath(dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if err := os.WriteFile(path, data[:len(data)-8], 0o600); err != nil {
		t.Fatalf("truncate journal: %v", err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()
	beginIntent(t, l2, "intent-3", "ord-3")

	entries, err := l2.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved: %v", err)
	}
	got := intentIDs(entries)
	if !hasID(got, "intent-1") {
		t.Errorf("intent-1 (before the torn tail) lost: %v", got)
	}
	if !hasID(got, "intent-3") {
		t.Errorf("intent-3 (appended after the torn tail) lost to concatenation corruption: %v", got)
	}
	if hasID(got, "intent-2") {
		t.Errorf("intent-2 was torn and should not be recovered as-is: %v", got)
	}
}

// TestTornTailQuarantinedToSidecar verifies the torn bytes are preserved (not
// destroyed) and the repair is surfaced via TornTails, so recovery is auditable.
func TestTornTailQuarantinedToSidecar(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	beginIntent(t, l, "keep-1", "ord-1")
	beginIntent(t, l, "torn-2", "ord-2")
	_ = l.Close()

	path := currentMonthPath(dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	// After truncation the torn tail is the partial line after the last newline.
	truncated := data[:len(data)-8]
	lastNL := bytes.LastIndexByte(truncated, '\n')
	tornTail := truncated[lastNL+1:]
	if err := os.WriteFile(path, truncated, 0o600); err != nil {
		t.Fatalf("truncate journal: %v", err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()

	repairs := l2.TornTails()
	if len(repairs) != 1 {
		t.Fatalf("TornTails = %+v, want exactly one repair", repairs)
	}
	if repairs[0].Bytes <= 0 {
		t.Errorf("repair recorded %d torn bytes, want > 0", repairs[0].Bytes)
	}
	sidecar := filepath.Join(dir, repairs[0].Sidecar)
	side, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("read sidecar %s: %v", sidecar, err)
	}
	if !strings.Contains(string(side), string(tornTail)) {
		t.Errorf("sidecar does not preserve the torn bytes %q; got:\n%s", tornTail, side)
	}
}

// TestUnresolvedSurfacesCorruptionLoudly is the F7 surfacing regression: a
// mid-file corrupt line must make Unresolved return a *RecoveryError (not a
// silent skip), while still returning the entries it could read.
func TestUnresolvedSurfacesCorruptionLoudly(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	beginIntent(t, l, "ok-1", "ord-1")
	beginIntent(t, l, "corrupt-2", "ord-2")
	beginIntent(t, l, "ok-3", "ord-3")
	_ = l.Close()

	// Bit-flip the middle line's content, keeping the trailing newline so this is
	// mid-file corruption, not a torn tail.
	path := currentMonthPath(dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	lines := splitLines(data)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	lines[1][len(lines[1])/2] ^= 0x01
	if err := os.WriteFile(path, joinLines(lines), 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()

	entries, err := l2.Unresolved()
	if err == nil {
		t.Fatal("Unresolved returned nil error despite a corrupt journal line (silent skip)")
	}
	var re *RecoveryError
	if !errors.As(err, &re) {
		t.Fatalf("error = %v, want *RecoveryError", err)
	}
	if len(re.Corruptions) != 1 {
		t.Errorf("RecoveryError.Corruptions = %d, want 1", len(re.Corruptions))
	}
	// The readable unresolved intents are still returned alongside the error.
	got := intentIDs(entries)
	if !hasID(got, "ok-1") || !hasID(got, "ok-3") {
		t.Errorf("readable intents = %v, want ok-1 and ok-3 recovered", got)
	}
}

// TestUnresolvedScansAllMonthsNotJustWindow is the F12 regression: an unresolved
// intent recorded months ago (outside the current+previous-month window) must
// still be surfaced for reconciliation. Fails on eabce2e, which only scanned two
// months.
func TestUnresolvedScansAllMonthsNotJustWindow(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = l.Close() }()

	now := time.Now().UTC()
	firstOfThisMonth := time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC)
	threeMonthsAgo := firstOfThisMonth.AddDate(0, -3, 0)

	fixedClock(l, threeMonthsAgo)
	beginIntent(t, l, "old-intent", "ord-old") // lands in a file 3 months back

	fixedClock(l, now)
	beginIntent(t, l, "new-intent", "ord-new") // rotates to the current month

	entries, err := l.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved: %v", err)
	}
	got := intentIDs(entries)
	if !hasID(got, "old-intent") {
		t.Errorf("old unresolved intent fell off the recovery horizon: %v", got)
	}
	if !hasID(got, "new-intent") {
		t.Errorf("current unresolved intent missing: %v", got)
	}
}

// TestWALWriteFailureIsSurfaced documents the F12 WAL contract on the ledger
// side: a stage write that fails must return an error so the caller can refuse
// to proceed to the network. (The ledger already surfaces this; the fix for
// callers that ignore it lives in the command layer.)
func TestWALWriteFailureIsSurfaced(t *testing.T) {
	l := openTemp(t)
	e, err := l.Begin(Intent{IntentID: "wal-1", Kind: "order.place", OrderID: "ord-1", Payload: samplePayload{OrderID: "ord-1"}})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// Break the journal handle so the next append's lock/write fails.
	_ = l.f.Close()
	if err := e.SendStarted(); err == nil {
		t.Fatal("SendStarted returned nil after the WAL write failed; the failure must be surfaced")
	}
}

// TestMkdirAllSyncFsyncsCreatedLevels is the F8 regression: creating the journal
// directory fsyncs each newly created directory level (so a power loss cannot
// drop the directory even though the file inside was fsynced). Exercised via the
// syncDir seam.
func TestMkdirAllSyncFsyncsCreatedLevels(t *testing.T) {
	var synced []string
	orig := syncDir
	syncDir = func(dir string) error {
		synced = append(synced, dir)
		return orig(dir)
	}
	defer func() { syncDir = orig }()

	base := t.TempDir()
	// Three new levels below an existing base, plus the journal file create.
	dir := filepath.Join(base, "a", "b", "journal")
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = l.Close() }()

	// Each created directory level's parent must have been fsynced: base (for a),
	// base/a (for b), base/a/b (for journal). The journal-file create fsyncs the
	// journal dir too.
	for _, want := range []string{base, filepath.Join(base, "a"), filepath.Join(base, "a", "b"), dir} {
		found := false
		for _, s := range synced {
			if s == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("directory %s was not fsynced during creation (synced=%v)", want, synced)
		}
	}
}

func beginEntry(t *testing.T, l *Ledger, intentID string) *Entry {
	t.Helper()
	e, err := l.Begin(Intent{
		IntentID: intentID, Kind: "order.place", AccountID: "acc", Profile: "test",
		Attempt: 1, OrderID: intentID, Payload: samplePayload{AccountID: "acc", OrderID: intentID},
	})
	if err != nil {
		t.Fatalf("Begin %s: %v", intentID, err)
	}
	return e
}

// TestUnresolvedSurfacesInterleavedDanglingLifecycle is the F8 regression: two
// concurrent lifecycles of one intent id — one crashed at send-started, the other
// confirmed — must leave the id unresolved. Last-write-wins would hide the
// dangling send behind the later confirmation.
func TestUnresolvedSurfacesInterleavedDanglingLifecycle(t *testing.T) {
	l := openTemp(t)

	a := beginEntry(t, l, "dup") // lifecycle A
	if err := a.SendStarted(); err != nil {
		t.Fatal(err)
	}
	b := beginEntry(t, l, "dup") // lifecycle B, same id
	if err := b.SendStarted(); err != nil {
		t.Fatal(err)
	}
	if err := b.Confirmed(Result{OrderID: "exch-dup"}); err != nil {
		t.Fatal(err)
	}

	un, err := l.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved: %v", err)
	}
	if !hasID(intentIDs(un), "dup") {
		t.Errorf("interleaved dangling lifecycle hidden: %v", intentIDs(un))
	}

	// A single, fully-confirmed lifecycle must NOT be reported unresolved.
	c := beginEntry(t, l, "clean")
	if err := c.SendStarted(); err != nil {
		t.Fatal(err)
	}
	if err := c.Confirmed(Result{OrderID: "exch-clean"}); err != nil {
		t.Fatal(err)
	}
	un2, err := l.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved: %v", err)
	}
	if hasID(intentIDs(un2), "clean") {
		t.Errorf("a fully-confirmed single lifecycle must not be unresolved: %v", intentIDs(un2))
	}
}

// TestRecoveryRepairsTornTailInOlderMonth is the F9 regression: a torn tail in an
// OLDER month file must be repaired by the recovery scan, not abort every
// reconciliation with a RecoveryError forever.
func TestRecoveryRepairsTornTailInOlderMonth(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	now := time.Now().UTC()
	firstOfThisMonth := time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC)
	old := firstOfThisMonth.AddDate(0, -3, 0)
	fixedClock(l, old)
	beginEntry(t, l, "old-keep")
	beginEntry(t, l, "old-torn")
	_ = l.Close()

	// Tear the tail of the OLD month file.
	oldPath := filepath.Join(dir, old.Format(monthFmt)+".jsonl")
	data, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatalf("read old journal: %v", err)
	}
	if err := os.WriteFile(oldPath, data[:len(data)-8], 0o600); err != nil {
		t.Fatalf("truncate old journal: %v", err)
	}

	l2, err := Open(dir) // current month; does not touch the old file
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()

	un, err := l2.Unresolved()
	if err != nil {
		t.Fatalf("recovery aborted on a torn tail in an older month: %v", err)
	}
	if !hasID(intentIDs(un), "old-keep") {
		t.Errorf("intact older-month intent lost during recovery: %v", intentIDs(un))
	}
}
