// Package ledger implements the write-ahead intent ledger described in
// plan-tinvest-cli.md §9 (reliability model) and §10 (intent ledger spec).
//
// Every mutation an agent asks the CLI to perform is journaled in stages,
// fsynced before each network step, so a crash at any point leaves a durable
// record that reconciliation can resolve — including the worst case where the
// broker accepted an order but the response was never recorded. The package is
// CLI-independent by design: it takes typed intents and results, never touches
// the network, and knows nothing about cobra, rendering, or gRPC.
//
// Storage is append-only JSONL at ${XDG_STATE_HOME:-~/.local/state}/tinvest/
// journal/YYYY-MM.jsonl (files 0600, directory 0700). Each line carries a
// crc32c (Castagnoli) checksum of its own content for corruption detection;
// corrupt lines are skipped, counted, and reported, never fatal to reading the
// lines that follow. Every append is fsynced (file, plus the parent directory
// on first create) and guarded by an advisory file lock (flock) so concurrent
// processes cannot interleave or tear a line. Monthly rotation only ever opens
// a new file; existing files are never rewritten.
package ledger

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Journal stages, in lifecycle order (plan §10). intent-created and
// send-started are the "unresolved" stages: an intent whose last recorded
// stage is one of these may have reached the broker and needs reconciliation.
const (
	StageIntentCreated   = "intent-created"
	StageSendStarted     = "send-started"
	StageBrokerConfirmed = "broker-confirmed"
	StageBrokerRejected  = "broker-rejected"
	StageReconciled      = "reconciled"
)

const (
	dirPerm  = 0o700
	filePerm = 0o600
	monthFmt = "2006-01"
)

// Intent is the durable description of a mutation, written at Begin before any
// network I/O. Per the idempotency contract (§9), the client-generated
// order_id lives inside Payload and is also surfaced in OrderID, so a crash
// between Begin and Confirmed leaves an Unresolved entry carrying the exact key
// that may have reached the broker.
//
// Payload must be the full request minus any token; the caller is responsible
// for stripping credentials before handing it to the ledger.
type Intent struct {
	IntentID    string // durable client intent key (agent-supplied, recommended)
	Kind        string // e.g. "order.place", "order.cancel", "stop.place"
	AccountID   string
	Profile     string
	Attempt     int
	OrderID     string // client order_id idempotency key, if applicable
	StopOrderID string
	Payload     any // full request minus token; JSON-marshalable
}

// Result carries broker/reconciliation outcome fields recorded on the
// confirmed/rejected/reconciled stages. Zero-valued fields are left unchanged.
type Result struct {
	OrderID         string
	StopOrderID     string
	ExchangeOrderID string
	TrackingID      string
	ExitCode        *int
	Error           string
}

// record is one JSONL line. Field order is fixed and load-bearing: the crc is
// computed over the marshaling of every field with crc itself zeroed, so
// marshaling must be deterministic (no maps).
type record struct {
	Seq             int64           `json:"seq"`
	TS              string          `json:"ts"`
	Stage           string          `json:"stage"`
	IntentID        string          `json:"intent_id"`
	Kind            string          `json:"kind"`
	AccountID       string          `json:"account_id"`
	Profile         string          `json:"profile"`
	Attempt         int             `json:"attempt"`
	PayloadHash     string          `json:"payload_hash"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	OrderID         string          `json:"order_id,omitempty"`
	StopOrderID     string          `json:"stop_order_id,omitempty"`
	ExchangeOrderID string          `json:"exchange_order_id,omitempty"`
	TrackingID      string          `json:"tracking_id,omitempty"`
	Error           string          `json:"error,omitempty"`
	ExitCode        *int            `json:"exit_code,omitempty"`
	CRC             uint32          `json:"crc"`
}

// marshal serializes the record with a trailing crc32c over the crc-zeroed
// form. Because the struct has fixed field order and no maps, the marshaling is
// deterministic and crcOK can reproduce it exactly.
func (r record) marshal(tbl *crc32.Table) ([]byte, error) {
	r.CRC = 0
	base, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	r.CRC = crc32.Checksum(base, tbl)
	return json.Marshal(r)
}

// crcOK re-derives the checksum the same way marshal produced it and compares.
func (r record) crcOK(tbl *crc32.Table) bool {
	stored := r.CRC
	r.CRC = 0
	base, err := json.Marshal(r)
	if err != nil {
		return false
	}
	return crc32.Checksum(base, tbl) == stored
}

// Ledger is an open handle to a journal directory. It is safe for concurrent
// use by multiple goroutines; appends are serialized and each is fsynced and
// flock-guarded before it returns.
type Ledger struct {
	dir string
	crc *crc32.Table
	now func() time.Time // overridable in tests for rotation

	mu    sync.Mutex
	f     *os.File
	month string // "2006-01" of the open file
	seq   int64  // process-monotonic, seeded from the current file at Open
}

// DefaultDir resolves ${XDG_STATE_HOME:-~/.local/state}/tinvest/journal.
func DefaultDir() (string, error) {
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("ledger: resolve home: %w", err)
		}
		base = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(base, "tinvest", "journal"), nil
}

// Open prepares the journal directory and opens the current month's file,
// creating both if needed. seq is seeded from the highest seq already present
// in that file so a process restart keeps producing increasing sequence
// numbers.
func Open(dir string) (*Ledger, error) {
	if dir == "" {
		return nil, errors.New("ledger: empty directory")
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("ledger: create directory: %w", err)
	}
	l := &Ledger{
		dir: dir,
		crc: crc32.MakeTable(crc32.Castagnoli),
		now: time.Now,
	}
	if err := l.openMonth(l.now().UTC()); err != nil {
		return nil, err
	}
	return l, nil
}

// Close releases the open file handle.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// openMonth switches the open file to the given month, creating it (and
// fsyncing the parent directory on first create) as necessary. Callers hold mu,
// except Open where the ledger is not yet shared.
func (l *Ledger) openMonth(t time.Time) error {
	month := t.UTC().Format(monthFmt)
	path := filepath.Join(l.dir, month+".jsonl")

	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("ledger: open %s: %w", path, err)
	}
	if created {
		if err := fsyncDir(l.dir); err != nil {
			_ = f.Close()
			return fmt.Errorf("ledger: fsync directory: %w", err)
		}
	}

	seq, err := maxSeq(path)
	if err != nil {
		_ = f.Close()
		return err
	}

	if l.f != nil {
		_ = l.f.Close()
	}
	l.f = f
	l.month = month
	if seq > l.seq {
		l.seq = seq
	}
	return nil
}

// append writes one record: it rotates the file if the month changed, assigns
// the seq and timestamp, then takes the flock, writes the line, and fsyncs
// before returning.
func (l *Ledger) append(r record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now().UTC()
	if month := now.Format(monthFmt); month != l.month {
		if err := l.openMonth(now); err != nil {
			return err
		}
	}

	l.seq++
	r.Seq = l.seq
	r.TS = now.Format(time.RFC3339Nano)

	line, err := r.marshal(l.crc)
	if err != nil {
		l.seq--
		return fmt.Errorf("ledger: marshal record: %w", err)
	}
	line = append(line, '\n')

	if err := lockFile(l.f); err != nil {
		l.seq--
		return fmt.Errorf("ledger: lock: %w", err)
	}
	defer func() { _ = unlockFile(l.f) }()

	if _, err := l.f.Write(line); err != nil {
		return fmt.Errorf("ledger: write: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("ledger: fsync: %w", err)
	}
	return nil
}

// Entry is a handle to a single journaled intent. It carries the latest known
// result fields forward across stages so each line is self-contained.
type Entry struct {
	l           *Ledger
	intent      Intent
	payloadHash string
	payload     json.RawMessage

	orderID         string
	stopOrderID     string
	exchangeOrderID string
	trackingID      string

	stage string
	seq   int64
}

// Begin records the intent-created stage and returns a handle. It MUST be
// called before any network send (§9): the payload it persists carries the
// client order_id, so a crash before Confirmed leaves an Unresolved entry with
// the exact key that may have reached the broker.
func (l *Ledger) Begin(intent Intent) (*Entry, error) {
	if intent.IntentID == "" {
		return nil, errors.New("ledger: intent id required")
	}
	payload, err := json.Marshal(intent.Payload)
	if err != nil {
		return nil, fmt.Errorf("ledger: marshal payload: %w", err)
	}
	sum := sha256.Sum256(payload)
	e := &Entry{
		l:           l,
		intent:      intent,
		payloadHash: hex.EncodeToString(sum[:]),
		payload:     payload,
		orderID:     intent.OrderID,
		stopOrderID: intent.StopOrderID,
	}
	if err := e.write(StageIntentCreated, nil); err != nil {
		return nil, err
	}
	return e, nil
}

// SendStarted records that the network send is about to happen. Fsynced before
// it returns so the "we may have sent it" fact is durable.
func (e *Entry) SendStarted() error { return e.write(StageSendStarted, nil) }

// Confirmed records a successful broker response and its result fields.
func (e *Entry) Confirmed(res Result) error {
	e.apply(res)
	return e.write(StageBrokerConfirmed, func(r *record) { r.ExitCode = res.ExitCode })
}

// Rejected records a definitive broker rejection.
func (e *Entry) Rejected(cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return e.write(StageBrokerRejected, func(r *record) { r.Error = msg })
}

// Reconciled records the outcome discovered by reconciliation, closing out an
// intent whose fate was previously unknown.
func (e *Entry) Reconciled(res Result) error {
	e.apply(res)
	return e.write(StageReconciled, func(r *record) {
		r.ExitCode = res.ExitCode
		r.Error = res.Error
	})
}

func (e *Entry) apply(res Result) {
	if res.OrderID != "" {
		e.orderID = res.OrderID
	}
	if res.StopOrderID != "" {
		e.stopOrderID = res.StopOrderID
	}
	if res.ExchangeOrderID != "" {
		e.exchangeOrderID = res.ExchangeOrderID
	}
	if res.TrackingID != "" {
		e.trackingID = res.TrackingID
	}
}

func (e *Entry) write(stage string, mut func(*record)) error {
	r := record{
		Stage:           stage,
		IntentID:        e.intent.IntentID,
		Kind:            e.intent.Kind,
		AccountID:       e.intent.AccountID,
		Profile:         e.intent.Profile,
		Attempt:         e.intent.Attempt,
		PayloadHash:     e.payloadHash,
		Payload:         e.payload,
		OrderID:         e.orderID,
		StopOrderID:     e.stopOrderID,
		ExchangeOrderID: e.exchangeOrderID,
		TrackingID:      e.trackingID,
	}
	if mut != nil {
		mut(&r)
	}
	if err := e.l.append(r); err != nil {
		return err
	}
	e.stage = stage
	return nil
}

// Accessors for callers reconstructing an intent from Unresolved.

func (e *Entry) IntentID() string         { return e.intent.IntentID }
func (e *Entry) Kind() string             { return e.intent.Kind }
func (e *Entry) AccountID() string        { return e.intent.AccountID }
func (e *Entry) Profile() string          { return e.intent.Profile }
func (e *Entry) Attempt() int             { return e.intent.Attempt }
func (e *Entry) OrderID() string          { return e.orderID }
func (e *Entry) StopOrderID() string      { return e.stopOrderID }
func (e *Entry) ExchangeOrderID() string  { return e.exchangeOrderID }
func (e *Entry) TrackingID() string       { return e.trackingID }
func (e *Entry) PayloadHash() string      { return e.payloadHash }
func (e *Entry) Payload() json.RawMessage { return e.payload }
func (e *Entry) Stage() string            { return e.stage }

// Unresolved scans the current and previous month files and returns a handle
// for every intent whose last recorded stage is intent-created or send-started
// — the intents that may have reached the broker and need reconciliation. The
// returned handles carry the order_id and payload from the journal, and can be
// closed out with Reconciled. Corrupt lines are skipped.
func (l *Ledger) Unresolved() ([]*Entry, error) {
	now := l.now().UTC()
	// Read previous month first, then current, so a stage recorded this month
	// supersedes one from last month for the same intent.
	files := []string{
		filepath.Join(l.dir, now.AddDate(0, -1, 0).Format(monthFmt)+".jsonl"),
		filepath.Join(l.dir, now.Format(monthFmt)+".jsonl"),
	}
	last := make(map[string]record)
	for _, path := range files {
		recs, _, err := readRecords(path, l.crc)
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			last[r.IntentID] = r
		}
	}

	var out []*Entry
	for _, r := range last {
		if r.Stage == StageIntentCreated || r.Stage == StageSendStarted {
			out = append(out, l.entryFromRecord(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out, nil
}

func (l *Ledger) entryFromRecord(r record) *Entry {
	return &Entry{
		l: l,
		intent: Intent{
			IntentID:    r.IntentID,
			Kind:        r.Kind,
			AccountID:   r.AccountID,
			Profile:     r.Profile,
			Attempt:     r.Attempt,
			OrderID:     r.OrderID,
			StopOrderID: r.StopOrderID,
		},
		payloadHash:     r.PayloadHash,
		payload:         r.Payload,
		orderID:         r.OrderID,
		stopOrderID:     r.StopOrderID,
		exchangeOrderID: r.ExchangeOrderID,
		trackingID:      r.TrackingID,
		stage:           r.Stage,
		seq:             r.Seq,
	}
}

// Corruption records one unreadable line found during a scan.
type Corruption struct {
	File   string `json:"file"`
	Line   int    `json:"line"`
	Reason string `json:"reason"`
}

// Report is the result of a Verify checksum scan.
type Report struct {
	Files       int          `json:"files"`
	Lines       int          `json:"lines"` // non-empty lines scanned (OK + Corrupt)
	OK          int          `json:"ok"`
	Corrupt     int          `json:"corrupt"`
	Corruptions []Corruption `json:"corruptions,omitempty"`
}

// Verify scans every journal file in the directory, validating each line's
// checksum. Corrupt lines are counted and reported but never abort the scan.
func (l *Ledger) Verify() (Report, error) {
	paths, err := filepath.Glob(filepath.Join(l.dir, "*.jsonl"))
	if err != nil {
		return Report{}, err
	}
	sort.Strings(paths)

	var rep Report
	for _, path := range paths {
		recs, corr, err := readRecords(path, l.crc)
		if err != nil {
			return rep, err
		}
		rep.Files++
		rep.OK += len(recs)
		rep.Corrupt += len(corr)
		rep.Corruptions = append(rep.Corruptions, corr...)
	}
	rep.Lines = rep.OK + rep.Corrupt
	return rep, nil
}

// readRecords reads a JSONL file line by line. A missing file yields no records
// and no error. Each line is skipped-and-reported if it is invalid JSON or its
// checksum does not verify; the reader continues to subsequent lines regardless.
func readRecords(path string, tbl *crc32.Table) ([]record, []Corruption, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("ledger: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	base := filepath.Base(path)
	br := bufio.NewReader(f) // ReadBytes grows past the buffer, so long lines are safe
	var (
		recs []record
		corr []Corruption
		line int
	)
	for {
		raw, readErr := br.ReadBytes('\n')
		if len(bytes.TrimSpace(raw)) > 0 {
			line++
			content := bytes.TrimRight(raw, "\n")
			var r record
			switch {
			case json.Unmarshal(content, &r) != nil:
				corr = append(corr, Corruption{File: base, Line: line, Reason: "invalid json"})
			case !r.crcOK(tbl):
				corr = append(corr, Corruption{File: base, Line: line, Reason: "crc mismatch"})
			default:
				recs = append(recs, r)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return recs, corr, fmt.Errorf("ledger: read %s: %w", path, readErr)
		}
	}
	return recs, corr, nil
}

// maxSeq returns the highest seq in a file, or 0 if the file is absent or
// empty. Corrupt lines are ignored for seeding purposes.
func maxSeq(path string) (int64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("ledger: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReader(f)
	var max int64
	for {
		raw, readErr := br.ReadBytes('\n')
		if len(bytes.TrimSpace(raw)) > 0 {
			var r struct {
				Seq int64 `json:"seq"`
			}
			if json.Unmarshal(bytes.TrimRight(raw, "\n"), &r) == nil && r.Seq > max {
				max = r.Seq
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return max, fmt.Errorf("ledger: read %s: %w", path, readErr)
		}
	}
	return max, nil
}
