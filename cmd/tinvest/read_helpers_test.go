package main

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/Dronnn/tinvest/internal/render"
	"github.com/Dronnn/tinvest/internal/transport"
)

func TestParseOptionalTimeRange(t *testing.T) {
	from, to, err := parseOptionalTimeRange("2026-01-01T00:00:00+04:00", "2026-01-02T00:00:00Z")
	if err != nil {
		t.Fatalf("parseOptionalTimeRange: %v", err)
	}
	if got := from.Format("2006-01-02T15:04:05Z07:00"); got != "2025-12-31T20:00:00Z" {
		t.Errorf("from = %s", got)
	}
	if got := to.Format("2006-01-02T15:04:05Z07:00"); got != "2026-01-02T00:00:00Z" {
		t.Errorf("to = %s", got)
	}
}

func TestEffectiveCallTimeout(t *testing.T) {
	if got := effectiveCallTimeout(0); got != transport.DefaultTimeout {
		t.Errorf("default timeout = %s, want %s", got, transport.DefaultTimeout)
	}
	if got := effectiveCallTimeout(45 * time.Second); got != 45*time.Second {
		t.Errorf("explicit timeout = %s", got)
	}
}

func TestClassifyHistoryErrorTreatsOutputPathAsUsage(t *testing.T) {
	err := &os.PathError{Op: "open", Path: "/not-a-directory/file.zip", Err: syscall.ENOTDIR}
	classified := classifyHistoryError(err)
	if classified.Code != render.CodeUsage || classified.ExitCode() != render.ExitUsage {
		t.Fatalf("classified = %+v", classified)
	}
}

func TestParseRequiredTimeRangeRejectsMissingAndReversed(t *testing.T) {
	if _, _, err := parseRequiredTimeRange("", "2026-01-02T00:00:00Z"); err == nil {
		t.Fatal("want missing --from error")
	}
	if _, _, err := parseRequiredTimeRange("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z"); err == nil {
		t.Fatal("want reversed range error")
	}
}
