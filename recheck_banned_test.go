package proxypool

import (
	"testing"
	"time"
)

func TestRecheckBannedFiltersAndKeepsReviveAt(t *testing.T) {
	p := NewPool()
	now := time.Now().UTC()
	keepRevive := now.Add(2 * time.Hour)
	p.cache.Set(ProxyState{URL: "http://a:8080", IsDead: true, Source: "one", FailReason: FailRefused, ReviveAt: keepRevive, LastCheckedAt: now})
	p.cache.Set(ProxyState{URL: "http://b:8080", IsDead: true, Source: "two", FailReason: FailTimeout, ReviveAt: now.Add(3 * time.Hour), LastCheckedAt: now})
	p.cache.Set(ProxyState{URL: "http://c:8080", IsDead: false, Source: "one", FailReason: FailRefused, ReviveAt: now.Add(4 * time.Hour), LastCheckedAt: now})

	if got := p.RecheckBanned(BanFilter{Reason: FailRefused, Source: "one"}); got != 1 {
		t.Fatalf("queued = %d, want 1", got)
	}
	if got := p.RecheckBanned(BanFilter{Reason: FailRefused, Source: "one"}); got != 0 {
		t.Fatalf("duplicate queueing count = %d, want 0", got)
	}

	task, ok := p.nextRefreshTask(now)
	if !ok || !task.forced || task.url != "http://a:8080" {
		t.Fatalf("forced task = %+v/%v", task, ok)
	}

	state, ok := p.cache.Get(task.url)
	if !ok {
		t.Fatal("banned proxy disappeared from cache")
	}
	applyFailureV2Config(&state, now.Add(time.Second), FailRefused, NetSnapshot{State: NetGood}, true, DefaultConfig())
	if !state.ReviveAt.Equal(keepRevive) {
		t.Fatalf("forced failure changed ReviveAt: got %s, want %s", state.ReviveAt, keepRevive)
	}
}
