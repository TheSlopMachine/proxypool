package proxypool

import (
	"testing"
	"time"
)

func TestNextModeHysteresis(t *testing.T) {
	cfg := DefaultConfig()
	if got := nextMode(ModeForeground, cfg.HighWater-1, cfg); got != ModeForeground {
		t.Fatalf("foreground below high water switched to %s", got)
	}
	if got := nextMode(ModeForeground, cfg.HighWater, cfg); got != ModeBackground {
		t.Fatalf("foreground at high water stayed %s", got)
	}
	if got := nextMode(ModeBackground, cfg.LowWater, cfg); got != ModeBackground {
		t.Fatalf("background at low water switched to %s", got)
	}
	if got := nextMode(ModeBackground, cfg.LowWater-1, cfg); got != ModeForeground {
		t.Fatalf("background below low water stayed %s", got)
	}
}

func TestStarvedRevivalPrefersSoftFailure(t *testing.T) {
	p := NewPool()
	p.sched.mu.Lock()
	p.sched.candQ = nil
	p.sched.candHead = 0
	p.sched.laneInflight = make(map[string]int)
	p.sched.inflight = make(map[string]struct{})
	p.sched.mu.Unlock()

	now := timeNowUTC()
	p.cache.Set(ProxyState{URL: "http://soft:8080", IsDead: true, FailReason: FailTimeout, ReviveAt: now.Add(time.Hour), LastCheckedAt: now})
	p.cache.Set(ProxyState{URL: "http://hard:8080", IsDead: true, FailReason: FailRefused, ReviveAt: now.Add(time.Minute), LastCheckedAt: now})
	p.starved.Store(true)

	task, ok := p.nextLaneTask(laneRevival, now)
	if !ok || task.url != "http://soft:8080" {
		t.Fatalf("starved revival selected %+v/%v, want soft failure first", task, ok)
	}
}

func timeNowUTC() time.Time {
	return time.Now().UTC()
}
