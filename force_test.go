package proxypool

import "testing"

func newForceTestPool(t *testing.T) *ProxyPool {
	t.Helper()
	pool, err := NewPool(newMemoryCache())
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestForceForegroundWithoutCandidatesIsNoop(t *testing.T) {
	pool := newForceTestPool(t)
	if n := pool.ForceForeground(); n != 0 {
		t.Fatalf("ForceForeground() = %d, want 0", n)
	}
	if pool.Snapshot().ForcedForeground {
		t.Fatal("override must not be set when the queue is empty")
	}
}

func TestForceForegroundOverridesBackgroundUntilDrained(t *testing.T) {
	pool := newForceTestPool(t)
	pool.storeMode(ModeBackground)
	pool.sched.queued["http://203.0.113.1:8080"] = struct{}{}
	pool.sched.queued["http://203.0.113.2:8080"] = struct{}{}

	if got := pool.laneMode(demandNeed{}); got != ModeBackground {
		t.Fatalf("laneMode before force = %s, want background", got)
	}
	if n := pool.ForceForeground(); n != 2 {
		t.Fatalf("ForceForeground() = %d, want 2", n)
	}
	if got := pool.laneMode(demandNeed{}); got != ModeForeground {
		t.Fatalf("laneMode while forced = %s, want foreground", got)
	}
	snap := pool.Snapshot()
	if !snap.ForcedForeground || snap.Mode != ModeBackground {
		t.Fatalf("snapshot = forced %v mode %s, want forced background-mode pool", snap.ForcedForeground, snap.Mode)
	}

	// One candidate still in flight keeps the override on.
	pool.sched.mu.Lock()
	pool.sched.queued = map[string]struct{}{}
	pool.sched.laneInflight[string(laneForeground)] = 1
	pool.sched.mu.Unlock()
	if !pool.Snapshot().ForcedForeground {
		t.Fatal("override must hold while a candidate check is in flight")
	}

	pool.sched.mu.Lock()
	pool.sched.laneInflight[string(laneForeground)] = 0
	pool.sched.mu.Unlock()
	if pool.Snapshot().ForcedForeground {
		t.Fatal("override must clear once the queue is drained")
	}
	if got := pool.laneMode(demandNeed{}); got != ModeBackground {
		t.Fatalf("laneMode after drain = %s, want background", got)
	}
}

func TestResetDropsQueuedCandidatesAndForce(t *testing.T) {
	pool := newForceTestPool(t)
	pool.sched.queued["http://203.0.113.1:8080"] = struct{}{}
	pool.sched.candQ = append(pool.sched.candQ, queuedCandidate{URL: "http://203.0.113.1:8080"})
	if pool.ForceForeground() != 1 {
		t.Fatal("force should see the queued candidate")
	}

	pool.Reset()

	snap := pool.Snapshot()
	if snap.Queued != 0 || snap.ForcedForeground {
		t.Fatalf("snapshot after reset = queued %d forced %v, want 0 and false", snap.Queued, snap.ForcedForeground)
	}
}
