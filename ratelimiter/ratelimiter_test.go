package ratelimiter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	const (
		tick  = 180 * time.Millisecond
		burst = 5
		delta = 30 * time.Millisecond // this one is required to mitigate flaky tests on slow systems
	)

	ctx, cancelCtx := context.WithCancel(t.Context())
	rateLimiter := New(ctx, tick, burst)
	assertLimitsWithBurst(t, rateLimiter, tick, delta, burst)

	// sleep tick * burst + extra 10 ms to make rateLimiter fill up again, and resetting burst
	t.Log("Sleeping to let rateLimiter fill up")
	time.Sleep(tick*burst + delta)
	assertLimitsWithBurst(t, rateLimiter, tick, delta, burst)

	t.Log("Sleeping to let rateLimiter fill up")
	time.Sleep(tick*burst + delta)

	t.Log("Cancelling context")
	cancelCtx()
	assert.EventuallyWithT(t, func(c *assert.CollectT) { assert.Empty(c, rateLimiter) }, delta, 5*time.Millisecond)
	// after ctx cancellation the channel must be drained and blocked forever
	assert.Never(t, func() bool {
		select {
		case <-rateLimiter:
			return true
		default:
			return false
		}
	}, time.Second, 500*time.Millisecond)
}

func assertLimitsWithBurst(t *testing.T, rateLimiter rateLimiter, tick, delta time.Duration, burst int) {
	for i := range 2 * burst {
		before := time.Now()

		select {
		case <-rateLimiter:
			after := time.Now()
			actualLimitDuration := after.Sub(before)
			t.Logf("rate limit duration: %q", actualLimitDuration)

			if i < burst {
				assert.WithinDuration(t, before, after, delta)
			} else {
				assert.InDelta(t, actualLimitDuration, tick, float64(delta))
			}
		case <-time.After(time.Second):
			assert.FailNow(t, "rateLimiter has been blocked too long")
		}
	}
}
