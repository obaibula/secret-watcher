package watcher

import (
	"context"
	"time"
)

type rateLimiter chan struct{}

// newRateLimiter returns rateLimiter with set limit, tick and burst.
// If ctx is done, rateLimiter will be drained and spawn will be stopped, so it will become ready for GC removal.
// It must not be used after ctx cancellation.
func newRateLimiter(ctx context.Context, tick time.Duration, burst int) rateLimiter {
	r := make(rateLimiter, burst)
	r.fillUpBurst()
	r.spawnRateLimiter(ctx, tick)
	return r
}

func (r rateLimiter) spawnRateLimiter(ctx context.Context, tick time.Duration) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				r.drain()
				return
			case <-time.Tick(tick):
				r <- struct{}{}
			}
		}
	}()
}

func (r rateLimiter) fillUpBurst() {
	for range cap(r) {
		r <- struct{}{}
	}
}

func (r rateLimiter) drain() {
	for range len(r) {
		<-r
	}
}
