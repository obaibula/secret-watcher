package ratelimiter

import (
	"context"
	"time"
)

type rateLimiter chan struct{}

// New returns rateLimiter with set limit, tick and burst.
// If ctx is done, rateLimiter will be drained and spawn will be stopped, so it will become ready for GC removal.
// It must not be used after ctx cancellation.
func New(ctx context.Context, tick time.Duration, burst int) rateLimiter {
	r := make(rateLimiter, burst)
	r.FillUpBurst()
	r.spawnRateLimiter(ctx, tick)

	return r
}

func (r rateLimiter) spawnRateLimiter(ctx context.Context, tick time.Duration) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				r.Drain()
				return
			case <-time.Tick(tick):
				r <- struct{}{}
			}
		}
	}()
}

func (r rateLimiter) FillUpBurst() {
	for range cap(r) {
		r <- struct{}{}
	}
}

func (r rateLimiter) Drain() {
	for {
		select {
		case <-r:

		default:
			return
		}
	}
}
