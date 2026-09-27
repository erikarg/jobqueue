package worker

import (
	"testing"
	"time"
)

func TestBackoffGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	base, maxDelay := 100*time.Millisecond, 2*time.Second
	ceilings := []time.Duration{
		100 * time.Millisecond, // attempt 1
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		2 * time.Second, // capped
		2 * time.Second,
	}

	for i, ceiling := range ceilings {
		attempt := i + 1
		for range 1000 {
			d := backoff(attempt, base, maxDelay)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, ceiling/2, ceiling)
			}
		}
	}
}

func TestBackoffDoesNotOverflowOnLargeAttempts(t *testing.T) {
	maxDelay := time.Hour
	if d := backoff(10_000, time.Second, maxDelay); d < maxDelay/2 || d > maxDelay {
		t.Fatalf("delay %v outside [%v, %v]", d, maxDelay/2, maxDelay)
	}
}
