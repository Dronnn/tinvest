package stream

import (
	"math/rand"
	"time"
)

const (
	defaultBackoffBase = 100 * time.Millisecond
	defaultBackoffCap  = 5 * time.Second
	defaultJitter      = 0.20
)

func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	multiplier := time.Duration(1 << min(attempt-1, 16))
	duration := defaultBackoffBase * multiplier
	if duration > defaultBackoffCap || duration <= 0 {
		duration = defaultBackoffCap
	}
	jitter := defaultJitter * (rand.Float64()*2 - 1)
	return time.Duration(float64(duration) * (1 + jitter))
}
