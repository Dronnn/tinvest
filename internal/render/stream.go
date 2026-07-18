package render

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
	"time"
)

// StreamEvent is the common NDJSON frame. Struct field order is intentional:
// type is the first key on every encoded line, followed by contract metadata.
type StreamEvent struct {
	Type          string     `json:"type"`
	SchemaVersion string     `json:"schema_version"`
	Time          string     `json:"time"`
	AccountID     string     `json:"account_id,omitempty"`
	Data          any        `json:"data,omitempty"`
	Error         *ErrorBody `json:"error,omitempty"`
}

// NewStreamEvent builds a frame with the required stream contract fields.
func NewStreamEvent(eventType string, at time.Time, data any) StreamEvent {
	return StreamEvent{
		Type: eventType, SchemaVersion: SchemaVersion,
		Time: at.UTC().Format(time.RFC3339Nano), Data: data,
	}
}

// NDJSONWriter writes and flushes one independent JSON object per line.
type NDJSONWriter struct {
	mu     sync.Mutex
	buffer *bufio.Writer
	encode *json.Encoder
}

func NewNDJSONWriter(w io.Writer) *NDJSONWriter {
	buffer := bufio.NewWriter(w)
	return &NDJSONWriter{buffer: buffer, encode: json.NewEncoder(buffer)}
}

func (w *NDJSONWriter) Write(event StreamEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if event.SchemaVersion == "" {
		event.SchemaVersion = SchemaVersion
	}
	if event.Time == "" {
		event.Time = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if err := w.encode.Encode(event); err != nil {
		return err
	}
	return w.buffer.Flush()
}

func (w *NDJSONWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Flush()
}
