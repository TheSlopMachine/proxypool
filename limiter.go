package proxypool

import (
	"context"
	"sync"
	"time"
)

// limiter is a dynamically sized semaphore. Lowering a limit does not revoke
// permits held by active checks; it only blocks future acquisitions.
type limiter struct {
	mu       sync.Mutex
	cond     *sync.Cond
	limit    int
	inflight int
}

func newLimiter(limit int) *limiter {
	l := &limiter{limit: max(1, limit)}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *limiter) Acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		for l.inflight >= l.limit {
			if err := ctx.Err(); err != nil {
				l.mu.Unlock()
				return err
			}
			// cond.Wait has no context form; wake periodically to observe cancellation.
			timer := time.AfterFunc(10*time.Millisecond, func() {
				l.mu.Lock()
				l.cond.Broadcast()
				l.mu.Unlock()
			})
			l.cond.Wait()
			if !timer.Stop() {
				// The timer callback may already be running; no action is required.
			}
		}
		if err := ctx.Err(); err != nil {
			l.mu.Unlock()
			return err
		}
		l.inflight++
		l.mu.Unlock()
		return nil
	}
}

func (l *limiter) Release() {
	l.mu.Lock()
	if l.inflight > 0 {
		l.inflight--
	}
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *limiter) SetLimit(n int) {
	if n < 1 {
		n = 1
	}
	l.mu.Lock()
	l.limit = n
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *limiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}

func (l *limiter) Inflight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight
}

// nextLimit applies one AIMD control step to the current limit. A good
// network sample must be observed twice consecutively before increasing the
// limit; degraded and down reset the good streak.
func nextLimit(limit int, state NetState, goodStreak int, cfg Config) (newLimit, newStreak int) {
	cfg = cfg.Resolve()
	limit = clampInt(limit, cfg.MinLimit, cfg.MaxLimit)
	switch state {
	case NetGood:
		goodStreak++
		if goodStreak >= 2 {
			increase := max(5, limit/20)
			newLimit = clampInt(limit+increase, cfg.MinLimit, cfg.MaxLimit)
			return newLimit, 0
		}
		return limit, goodStreak
	case NetDegraded:
		return clampInt(limit/2, cfg.MinLimit, cfg.MaxLimit), 0
	case NetDown:
		return cfg.MinLimit, 0
	default:
		return limit, 0
	}
}
