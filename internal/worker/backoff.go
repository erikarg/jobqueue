package worker

import (
	"math/rand/v2"
	"time"
)

// backoff returns the delay before retrying after the given (1-based) attempt.
//
// The ceiling grows exponentially, base * 2^(attempt-1), capped at maxDelay. The
// result is picked uniformly from [ceiling/2, ceiling] ("equal jitter") so that
// jobs failing together (e.g. a downstream outage) don't retry in lockstep.
func backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	ceiling := base
	for i := 1; i < attempt && ceiling < maxDelay; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, maxDelay)

	half := ceiling / 2
	return half + rand.N(ceiling-half+1)
}
