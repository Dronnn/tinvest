package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	investapi "tinvest/internal/pb/investapi"
	"tinvest/internal/render"
	streamrunner "tinvest/internal/stream"
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

func TestTimestampLessOrderBookSnapshotKeepsAllLiveFrames(t *testing.T) {
	var output bytes.Buffer
	writer := render.NewNDJSONWriter(&output)
	first := &investapi.MarketDataResponse{
		Payload: &investapi.MarketDataResponse_Orderbook{Orderbook: &investapi.OrderBook{InstrumentUid: "uid-1", Depth: 10}},
	}
	second := &investapi.MarketDataResponse{
		Payload: &investapi.MarketDataResponse_Orderbook{Orderbook: &investapi.OrderBook{InstrumentUid: "uid-1", Depth: 20}},
	}
	firstDelivered := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	index := 0
	runner := streamrunner.Runner[investapi.MarketDataRequest, investapi.MarketDataResponse]{
		Open: func(streamCtx context.Context) (streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse], error) {
			return streamrunner.Session[investapi.MarketDataRequest, investapi.MarketDataResponse]{Recv: func() (*investapi.MarketDataResponse, error) {
				switch index {
				case 0:
					index++
					return first, nil
				case 1:
					<-firstDelivered
					index++
					return second, nil
				default:
					<-streamCtx.Done()
					return nil, streamCtx.Err()
				}
			}}, nil
		},
		Watchdog: time.Second,
		OnMessage: func(response *investapi.MarketDataResponse) error {
			if err := writer.Write(render.MarketDataStreamEvent(response, time.Now())); err != nil {
				return err
			}
			if response == first {
				close(firstDelivered)
			}
			if response == second {
				cancel()
			}
			return nil
		},
	}
	configureOrderBookReconciliation(
		&runner, []string{"uid-1"}, 20,
		func(context.Context, string, int32) (*investapi.GetOrderBookResponse, error) {
			// Leave time for the first frame to enter the runner's reconciliation
			// buffer, so the test exercises both buffered and later live filtering.
			time.Sleep(20 * time.Millisecond)
			return &investapi.GetOrderBookResponse{InstrumentUid: "uid-1", Depth: 20}, nil
		},
		func(snapshot *investapi.GetOrderBookResponse) error {
			return writer.Write(render.NewStreamEvent("snapshot", time.Now(), render.OrderBook(snapshot)))
		},
	)
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("output has %d events, want snapshot plus two order books: %s", len(lines), output.String())
	}
	wantTypes := []string{"snapshot", "orderbook", "orderbook"}
	for i, line := range lines {
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("event %d is invalid JSON: %v", i, err)
		}
		if event.Type != wantTypes[i] {
			t.Fatalf("event %d type = %q, want %q; output: %s", i, event.Type, wantTypes[i], output.String())
		}
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
