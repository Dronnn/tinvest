package ledger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// samplePayload is a stand-in for an order request minus token.
type samplePayload struct {
	AccountID string `json:"account_id"`
	OrderID   string `json:"order_id"`
	Quantity  int64  `json:"quantity"`
	Direction string `json:"direction"`
}

func openTemp(t *testing.T) *Ledger {
	t.Helper()
	l, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func fixedClock(l *Ledger, ts time.Time) {
	l.now = func() time.Time { return ts }
}

func TestJournalHandleRetainsReadAccessForFileLocking(t *testing.T) {
	l := openTemp(t)
	if _, err := l.f.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read empty journal: %v, want EOF", err)
	}
}

func TestAppendReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	e, err := l.Begin(Intent{
		IntentID:  "intent-1",
		Kind:      "order.place",
		AccountID: "acc-1",
		Profile:   "sandbox",
		Attempt:   1,
		OrderID:   "ord-1",
		Payload:   samplePayload{AccountID: "acc-1", OrderID: "ord-1", Quantity: 3, Direction: "buy"},
	})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := e.SendStarted(); err != nil {
		t.Fatalf("SendStarted: %v", err)
	}
	if err := e.Confirmed(Result{OrderID: "ord-1", ExchangeOrderID: "exch-9", TrackingID: "trk-1"}); err != nil {
		t.Fatalf("Confirmed: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify integrity + content.
	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()

	rep, err := l2.Verify()
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if rep.OK != 3 || rep.Corrupt != 0 || rep.Lines != 3 {
		t.Fatalf("report = %+v, want 3 ok / 0 corrupt", rep)
	}

	recs, corr, err := readRecords(filepath.Join(dir, l2.now().UTC().Format(monthFmt)+".jsonl"), l2.crc)
	if err != nil {
		t.Fatalf("readRecords: %v", err)
	}
	if len(corr) != 0 {
		t.Fatalf("corruptions = %v", corr)
	}
	if len(recs) != 3 {
		t.Fatalf("records = %d, want 3", len(recs))
	}
	wantStages := []string{StageIntentCreated, StageSendStarted, StageBrokerConfirmed}
	for i, r := range recs {
		if r.Stage != wantStages[i] {
			t.Errorf("record %d stage = %q, want %q", i, r.Stage, wantStages[i])
		}
		if r.Seq != int64(i+1) {
			t.Errorf("record %d seq = %d, want %d", i, r.Seq, i+1)
		}
		if r.IntentID != "intent-1" {
			t.Errorf("record %d intent_id = %q", i, r.IntentID)
		}
	}
	if recs[2].ExchangeOrderID != "exch-9" || recs[2].TrackingID != "trk-1" {
		t.Errorf("confirmed line missing result fields: %+v", recs[2])
	}
	// Payload survives round-trip and matches what we passed.
	var pl samplePayload
	if err := json.Unmarshal(recs[0].Payload, &pl); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if pl.OrderID != "ord-1" || pl.Quantity != 3 {
		t.Errorf("payload = %+v", pl)
	}
}

// TestDurableAfterAppend exercises the write+fsync path: the bytes are readable
// by an independent reader immediately after each stage returns, without Close.
func TestDurableAfterAppend(t *testing.T) {
	l := openTemp(t)
	e, err := l.Begin(Intent{IntentID: "d1", Kind: "order.place", Payload: samplePayload{OrderID: "d1"}})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	path := filepath.Join(l.dir, l.now().UTC().Format(monthFmt)+".jsonl")

	recs, _, err := readRecords(path, l.crc)
	if err != nil {
		t.Fatalf("readRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].Stage != StageIntentCreated {
		t.Fatalf("after Begin, records = %+v", recs)
	}

	if err := e.SendStarted(); err != nil {
		t.Fatalf("SendStarted: %v", err)
	}
	recs, _, err = readRecords(path, l.crc)
	if err != nil {
		t.Fatalf("readRecords: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("after SendStarted, records = %d, want 2", len(recs))
	}
}

// TestCrashLeavesUnresolvedWithOrderID simulates a crash between Begin and
// Confirmed: we write intent-created only, "kill" by not writing further
// stages, reopen, and assert Unresolved returns the intent with its order_id.
func TestCrashLeavesUnresolvedWithOrderID(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := l.Begin(Intent{
		IntentID: "crash-1", Kind: "order.place", AccountID: "acc", OrderID: "ord-crash",
		Payload: samplePayload{OrderID: "ord-crash", Quantity: 5},
	}); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// Simulate crash: drop the handle without further stages.
	_ = l.Close()

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()

	un, err := l2.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved: %v", err)
	}
	if len(un) != 1 {
		t.Fatalf("unresolved = %d, want 1", len(un))
	}
	got := un[0]
	if got.IntentID() != "crash-1" || got.OrderID() != "ord-crash" {
		t.Fatalf("unresolved intent = %s / order %s", got.IntentID(), got.OrderID())
	}
	if got.Stage() != StageIntentCreated {
		t.Errorf("stage = %q, want intent-created", got.Stage())
	}
	// The payload carrying the idempotency key survives for reconciliation.
	var pl samplePayload
	if err := json.Unmarshal(got.Payload(), &pl); err != nil || pl.OrderID != "ord-crash" {
		t.Errorf("payload = %s err=%v", got.Payload(), err)
	}

	// Closing it out via Reconciled removes it from Unresolved.
	code := 0
	if err := got.Reconciled(Result{OrderID: "ord-crash", ExitCode: &code}); err != nil {
		t.Fatalf("Reconciled: %v", err)
	}
	un2, err := l2.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved after reconcile: %v", err)
	}
	if len(un2) != 0 {
		t.Fatalf("still unresolved after reconcile: %d", len(un2))
	}
}

// TestSendStartedIsUnresolved confirms send-started (sent, outcome unknown) is
// also treated as unresolved, while a confirmed intent is not.
func TestSendStartedIsUnresolved(t *testing.T) {
	l := openTemp(t)

	e1, _ := l.Begin(Intent{IntentID: "s1", Kind: "k", OrderID: "o1", Payload: map[string]string{"id": "o1"}})
	if err := e1.SendStarted(); err != nil {
		t.Fatalf("SendStarted: %v", err)
	}
	e2, _ := l.Begin(Intent{IntentID: "s2", Kind: "k", OrderID: "o2", Payload: map[string]string{"id": "o2"}})
	if err := e2.SendStarted(); err != nil {
		t.Fatalf("SendStarted: %v", err)
	}
	if err := e2.Confirmed(Result{OrderID: "o2"}); err != nil {
		t.Fatalf("Confirmed: %v", err)
	}

	un, err := l.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved: %v", err)
	}
	if len(un) != 1 || un[0].IntentID() != "s1" {
		t.Fatalf("unresolved = %v, want only s1", intentIDs(un))
	}
}

func intentIDs(es []*Entry) []string {
	ids := make([]string, len(es))
	for i, e := range es {
		ids[i] = e.IntentID()
	}
	return ids
}

// TestCorruptLineSkippedAndReported flips a bit in the middle line and asserts
// it is skipped + reported, while the lines around it still read.
func TestCorruptLineSkippedAndReported(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := l.Begin(Intent{
			IntentID: fmt.Sprintf("c%d", i), Kind: "order.place",
			Payload: samplePayload{OrderID: fmt.Sprintf("c%d", i), Quantity: int64(i)},
		}); err != nil {
			t.Fatalf("Begin %d: %v", i, err)
		}
	}
	_ = l.Close()

	path := filepath.Join(dir, time.Now().UTC().Format(monthFmt)+".jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	// Flip a bit inside the second line's content.
	lines := splitLines(data)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	mid := lines[1]
	mid[len(mid)/2] ^= 0x01 // bit flip
	corrupted := joinLines(lines)
	if err := os.WriteFile(path, corrupted, 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	l2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = l2.Close() }()

	rep, err := l2.Verify()
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if rep.Corrupt != 1 {
		t.Fatalf("corrupt = %d, want 1 (report %+v)", rep.Corrupt, rep)
	}
	if rep.OK != 2 {
		t.Fatalf("ok = %d, want 2 (later lines must still read)", rep.OK)
	}
	if len(rep.Corruptions) != 1 || rep.Corruptions[0].Line != 2 {
		t.Fatalf("corruption detail = %+v, want line 2", rep.Corruptions)
	}
	// The two surviving records are readable.
	recs, _, err := readRecords(path, l2.crc)
	if err != nil {
		t.Fatalf("readRecords: %v", err)
	}
	if len(recs) != 2 || recs[0].IntentID != "c0" || recs[1].IntentID != "c2" {
		t.Fatalf("survivors = %v, want c0,c2", intentIDsOf(recs))
	}
}

func intentIDsOf(recs []record) []string {
	ids := make([]string, len(recs))
	for i, r := range recs {
		ids[i] = r.IntentID
	}
	return ids
}

func splitLines(data []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			out = append(out, append([]byte(nil), data[start:i]...))
			start = i + 1
		}
	}
	if start < len(data) {
		out = append(out, append([]byte(nil), data[start:]...))
	}
	return out
}

func joinLines(lines [][]byte) []byte {
	var out []byte
	for _, l := range lines {
		out = append(out, l...)
		out = append(out, '\n')
	}
	return out
}

// TestConcurrentAppendsGoroutines exercises the in-process mutex + flock path:
// many goroutines appending at once produce no corrupt or torn lines and a
// unique, gap-free set of sequence numbers.
func TestConcurrentAppendsGoroutines(t *testing.T) {
	l := openTemp(t)

	const goroutines, per = 8, 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := fmt.Sprintf("g%d-%d", g, i)
				e, err := l.Begin(Intent{IntentID: id, Kind: "order.place", Payload: map[string]int{"g": g, "i": i}})
				if err != nil {
					t.Errorf("Begin %s: %v", id, err)
					return
				}
				if err := e.Confirmed(Result{OrderID: id}); err != nil {
					t.Errorf("Confirmed %s: %v", id, err)
				}
			}
		}(g)
	}
	wg.Wait()

	rep, err := l.Verify()
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	want := goroutines * per * 2 // Begin + Confirmed per intent
	if rep.OK != want || rep.Corrupt != 0 {
		t.Fatalf("report = %+v, want %d ok / 0 corrupt", rep, want)
	}

	// Sequence numbers are unique and contiguous 1..want.
	recs, _, err := readRecords(filepath.Join(l.dir, l.now().UTC().Format(monthFmt)+".jsonl"), l.crc)
	if err != nil {
		t.Fatalf("readRecords: %v", err)
	}
	seen := make(map[int64]bool, len(recs))
	for _, r := range recs {
		if seen[r.Seq] {
			t.Fatalf("duplicate seq %d", r.Seq)
		}
		seen[r.Seq] = true
	}
	for s := int64(1); s <= int64(want); s++ {
		if !seen[s] {
			t.Fatalf("missing seq %d", s)
		}
	}
}

// TestMonthRotation drives the clock across a month boundary and asserts a new
// file is opened without touching the old one.
func TestMonthRotation(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = l.Close() }()

	jan := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 3, 9, 0, 0, 0, time.UTC)

	fixedClock(l, jan)
	if _, err := l.Begin(Intent{IntentID: "jan-1", Kind: "k", Payload: map[string]string{"m": "jan"}}); err != nil {
		t.Fatalf("Begin jan: %v", err)
	}
	fixedClock(l, feb)
	if _, err := l.Begin(Intent{IntentID: "feb-1", Kind: "k", Payload: map[string]string{"m": "feb"}}); err != nil {
		t.Fatalf("Begin feb: %v", err)
	}

	janPath := filepath.Join(dir, "2026-01.jsonl")
	febPath := filepath.Join(dir, "2026-02.jsonl")
	janRecs, _, err := readRecords(janPath, l.crc)
	if err != nil {
		t.Fatalf("read jan: %v", err)
	}
	febRecs, _, err := readRecords(febPath, l.crc)
	if err != nil {
		t.Fatalf("read feb: %v", err)
	}
	if len(janRecs) != 1 || janRecs[0].IntentID != "jan-1" {
		t.Fatalf("jan file = %v", intentIDsOf(janRecs))
	}
	if len(febRecs) != 1 || febRecs[0].IntentID != "feb-1" {
		t.Fatalf("feb file = %v", intentIDsOf(febRecs))
	}
	// seq keeps climbing across the rotation.
	if febRecs[0].Seq <= janRecs[0].Seq {
		t.Errorf("seq did not advance across rotation: jan %d feb %d", janRecs[0].Seq, febRecs[0].Seq)
	}

	// Unresolved spans current + previous month.
	fixedClock(l, feb)
	un, err := l.Unresolved()
	if err != nil {
		t.Fatalf("Unresolved: %v", err)
	}
	if len(un) != 2 {
		t.Fatalf("unresolved across months = %d, want 2", len(un))
	}
}

// TestLargeFileScan is a performance/correctness sanity check over 10k lines.
func TestLargeFileScan(t *testing.T) {
	l := openTemp(t)
	const n = 10000
	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := l.Begin(Intent{
			IntentID: fmt.Sprintf("big-%d", i), Kind: "order.place",
			Payload: samplePayload{OrderID: fmt.Sprintf("big-%d", i), Quantity: int64(i)},
		}); err != nil {
			t.Fatalf("Begin %d: %v", i, err)
		}
	}
	writeElapsed := time.Since(start)

	scanStart := time.Now()
	rep, err := l.Verify()
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	scanElapsed := time.Since(scanStart)

	if rep.OK != n || rep.Corrupt != 0 {
		t.Fatalf("report = %+v, want %d ok / 0 corrupt", rep, n)
	}
	if testing.Verbose() {
		t.Logf("10k appends in %v, scan in %v", writeElapsed, scanElapsed)
	}
	if scanElapsed > 10*time.Second {
		t.Errorf("scan of %d lines took %v, unexpectedly slow", n, scanElapsed)
	}
}
