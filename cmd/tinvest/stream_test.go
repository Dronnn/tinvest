package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"tinvest/internal/render"
)

func TestStreamValidationFailureIsOneNDJSONEvent(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("TINVEST_TOKEN", "")
	application := &app{}
	root := application.rootCmd()
	root.SetArgs([]string{"stream", "marketdata", "--trades"})

	var executionErr error
	output := captureStdout(t, func() { executionErr = root.Execute() })
	var exit *exitError
	if !asExitError(executionErr, &exit) || exit.code != render.ExitUsage {
		t.Fatalf("error = %v, want exit 2", executionErr)
	}
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 1 {
		t.Fatalf("output has %d lines, want one NDJSON object: %q", len(lines), output)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("invalid NDJSON: %v", err)
	}
	if event["type"] != "error" || event["schema_version"] != render.SchemaVersion || event["time"] == nil {
		t.Fatalf("event = %#v", event)
	}
}

func TestCobraStreamFailureIsOneNDJSONEvent(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"tinvest", "stream", "marketdata", "--definitely-unknown"}
	t.Cleanup(func() { os.Args = oldArgs })

	var code int
	output := captureStdout(t, func() { code = executeContext(context.Background()) })
	if code != render.ExitUsage {
		t.Fatalf("exit = %d, want 2", code)
	}
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 1 {
		t.Fatalf("output has %d lines, want one NDJSON object: %q", len(lines), output)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("invalid NDJSON: %v", err)
	}
	if event["type"] != "error" || event["schema_version"] != render.SchemaVersion || event["time"] == nil {
		t.Fatalf("event = %#v", event)
	}
}

func TestCanceledStreamEmitsFinalEventAndSucceeds(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	application := &app{}
	root := application.rootCmd()
	root.SetArgs([]string{"stream", "marketdata", "--instrument", "uid", "--trades"})

	var executionErr error
	output := captureStdout(t, func() { executionErr = root.ExecuteContext(ctx) })
	if executionErr != nil {
		t.Fatalf("ExecuteContext: %v", executionErr)
	}
	var event struct {
		Type string `json:"type"`
		Data struct {
			Reason string `json:"reason"`
			Final  bool   `json:"final"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &event); err != nil {
		t.Fatalf("invalid final NDJSON event: %v", err)
	}
	if event.Type != "disconnected" || event.Data.Reason != "shutdown" || !event.Data.Final {
		t.Fatalf("event = %+v, want final shutdown", event)
	}
}

func TestStreamInvocationRecognizesPersistentFlagForms(t *testing.T) {
	for _, args := range [][]string{
		{"-ojson", "stream", "marketdata", "--bad"},
		{"--sandbox=true", "stream", "marketdata", "--bad"},
		{"--no-rate-limit=true", "stream", "marketdata", "--bad"},
		{"--profile", "main", "stream", "marketdata", "--bad"},
	} {
		if !isStreamInvocation(args) {
			t.Errorf("isStreamInvocation(%v) = false", args)
		}
	}
}

func TestSubscriptionCapFailsBeforeTokenOrBrokerCall(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("TINVEST_TOKEN", "")
	args := []string{"stream", "marketdata", "--candles=1m", "--orderbook=20", "--trades", "--last-price", "--info"}
	for i := 0; i < 61; i++ {
		args = append(args, "--instrument", "uid-"+strconv.Itoa(i))
	}
	application := &app{}
	root := application.rootCmd()
	root.SetArgs(args)
	var executionErr error
	output := captureStdout(t, func() { executionErr = root.Execute() })
	var exit *exitError
	if !asExitError(executionErr, &exit) || exit.code != render.ExitUsage {
		t.Fatalf("error = %v, want local usage exit 2", executionErr)
	}
	if !strings.Contains(output, "too many subscriptions") || strings.Contains(output, "no token configured") {
		t.Fatalf("output = %q, want local cap validation before auth", output)
	}
}

func asExitError(err error, target **exitError) bool {
	if err == nil {
		return false
	}
	value, ok := err.(*exitError)
	if ok {
		*target = value
	}
	return ok
}
