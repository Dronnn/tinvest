package render

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNDJSONEventsAreStandaloneAndMatchGolden(t *testing.T) {
	var output bytes.Buffer
	writer := NewNDJSONWriter(&output)
	now := time.Date(2026, 7, 19, 12, 34, 56, 0, time.UTC)
	events := []StreamEvent{
		NewStreamEvent("connected", now, map[string]any{"attempt": 1, "subscriptions": 2}),
		NewStreamEvent("snapshot", now.Add(time.Second), map[string]any{"instrument_uid": "uid-1", "depth": 20}),
		NewStreamEvent("disconnected", now.Add(2*time.Second), map[string]any{"reason": "shutdown", "final": true}),
	}
	for _, event := range events {
		if err := writer.Write(event); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != len(events) {
		t.Fatalf("lines = %d, want %d\n%s", len(lines), len(events), output.String())
	}
	for index, line := range lines {
		if !strings.HasPrefix(line, `{"type":`) {
			t.Errorf("line %d does not place type first: %s", index, line)
		}
		var object map[string]any
		if err := json.Unmarshal([]byte(line), &object); err != nil {
			t.Fatalf("line %d is not standalone JSON: %v", index, err)
		}
		for _, field := range []string{"type", "schema_version", "time"} {
			if object[field] == "" || object[field] == nil {
				t.Errorf("line %d missing required %s: %s", index, field, line)
			}
		}
		if _, err := time.Parse(time.RFC3339Nano, object["time"].(string)); err != nil {
			t.Errorf("line %d time is not RFC3339: %v", index, err)
		}
	}

	want, err := os.ReadFile("testdata/stream.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != strings.TrimSpace(string(want)) {
		t.Errorf("NDJSON mismatch:\n--- got ---\n%s--- want ---\n%s", output.String(), want)
	}
}
