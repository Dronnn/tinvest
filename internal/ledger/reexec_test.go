package ledger

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

// Environment used to switch a re-exec of the test binary into "child" mode.
const (
	envChildDir   = "LEDGER_TEST_CHILD_DIR"
	envChildID    = "LEDGER_TEST_CHILD_ID"
	envChildCount = "LEDGER_TEST_CHILD_COUNT"
)

// TestInterprocessAppends verifies the flock path across real OS processes: it
// re-execs this test binary several times, each child appending into the same
// journal directory, then asserts every line is intact (no torn/corrupt lines
// from interleaving) and the expected total is present.
//
// This uses the standard `go test` re-exec pattern: the parent spawns
// os.Args[0] with envChildDir set; the child branch below runs the appends and
// exits before the test framework proceeds.
func TestInterprocessAppends(t *testing.T) {
	if dir := os.Getenv(envChildDir); dir != "" {
		runChildAppends(dir)
		return // unreachable: runChildAppends calls os.Exit
	}

	dir := t.TempDir()
	const children, per = 4, 40

	cmds := make([]*exec.Cmd, children)
	for i := range cmds {
		cmd := exec.Command(os.Args[0], "-test.run", "^TestInterprocessAppends$", "-test.v")
		cmd.Env = append(os.Environ(),
			envChildDir+"="+dir,
			envChildID+"="+strconv.Itoa(i),
			envChildCount+"="+strconv.Itoa(per),
		)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		cmds[i] = cmd
	}
	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatalf("start child %d: %v", i, err)
		}
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("child %d failed: %v", i, err)
		}
	}

	l, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = l.Close() }()

	rep, err := l.Verify()
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	want := children * per
	if rep.OK != want || rep.Corrupt != 0 {
		t.Fatalf("report = %+v, want %d ok / 0 corrupt (interleaving would tear lines)", rep, want)
	}
}

// runChildAppends is the child branch: append envChildCount intents, then exit.
func runChildAppends(dir string) {
	id := os.Getenv(envChildID)
	count, _ := strconv.Atoi(os.Getenv(envChildCount))

	l, err := Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child %s: Open: %v\n", id, err)
		os.Exit(1)
	}
	for i := 0; i < count; i++ {
		intentID := fmt.Sprintf("child-%s-%d", id, i)
		if _, err := l.Begin(Intent{
			IntentID: intentID,
			Kind:     "order.place",
			Profile:  "sandbox",
			OrderID:  intentID,
			Payload:  map[string]any{"child": id, "i": i},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "child %s: Begin: %v\n", id, err)
			os.Exit(1)
		}
	}
	_ = l.Close()
	os.Exit(0)
}
