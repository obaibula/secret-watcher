package watcher

import (
	"context"
	"time"
)

const (
	rateLimitTime  = time.Second
	rateLimitBurst = 3
)

type rateLimiter chan struct{}

func newRateLimiter(ctx context.Context) rateLimiter {
	r := make(rateLimiter, rateLimitBurst)
	r.ResetBurst()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.Tick(rateLimitTime):
				r <- struct{}{}
			}
		}
	}()
	return r
}

func (r rateLimiter) ResetBurst() {
	for range rateLimitBurst {
		r <- struct{}{}
	}
}
