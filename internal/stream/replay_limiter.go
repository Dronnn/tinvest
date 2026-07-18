package stream

import (
	"context"
	"fmt"
	"time"
)

const (
	DefaultReplayLimit  = 100
	DefaultReplayWindow = time.Minute
)

// replayLimiter enforces the broker's strict rolling cap on subscription
// SendMsg calls across reconnects. It intentionally counts setup messages
// such as ping settings too, which is conservative and keeps the generic
// runner independent of protobuf request shapes.
type replayLimiter struct {
	limit      int
	window     time.Duration
	timestamps []time.Time
}

func newReplayLimiter(limit int, window time.Duration) *replayLimiter {
	if limit <= 0 {
		limit = DefaultReplayLimit
	}
	if window <= 0 {
		window = DefaultReplayWindow
	}
	return &replayLimiter{limit: limit, window: window}
}

func (l *replayLimiter) waitN(ctx context.Context, count int) error {
	if count <= 0 {
		return nil
	}
	if count > l.limit {
		return fmt.Errorf("subscription replay has %d requests, exceeds rolling limit %d", count, l.limit)
	}
	for {
		now := time.Now()
		cutoff := now.Add(-l.window)
		firstActive := 0
		for firstActive < len(l.timestamps) && !l.timestamps[firstActive].After(cutoff) {
			firstActive++
		}
		l.timestamps = l.timestamps[firstActive:]
		if len(l.timestamps)+count <= l.limit {
			for range count {
				l.timestamps = append(l.timestamps, now)
			}
			return nil
		}
		needed := len(l.timestamps) + count - l.limit
		readyAt := l.timestamps[needed-1].Add(l.window)
		if err := wait(ctx, time.Until(readyAt)); err != nil {
			return err
		}
	}
}
