package proxypool

import (
	"reflect"
	"testing"
	"time"
)

func TestApplyFailureV2(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now().UTC()
	base := ProxyState{
		URL:           "http://127.0.0.1:8080",
		Score:         8,
		Penalty:       2,
		SoftFails:     3,
		IsDead:        false,
		Suspect:       false,
		ReviveAt:      now.Add(time.Hour),
		LastCheckedAt: now.Add(-time.Minute),
	}

	t.Run("forced", func(t *testing.T) {
		state := base
		old := state
		applyFailureV2(&state, now, FailRefused, NetSnapshot{State: NetDown}, true)
		if state.FailReason != FailRefused || !state.LastCheckedAt.Equal(now) {
			t.Fatalf("forced metadata not updated: %+v", state)
		}
		state.FailReason = old.FailReason
		state.LastCheckedAt = old.LastCheckedAt
		old.ReviveAt = state.ReviveAt
		if !reflect.DeepEqual(state, old) {
			t.Fatalf("forced check changed protected fields: before=%+v after=%+v", old, state)
		}
	})

	t.Run("soft while network down", func(t *testing.T) {
		state := base
		before := state
		applyFailureV2(&state, now, FailTimeout, NetSnapshot{State: NetDown}, false)
		if !reflect.DeepEqual(state, before) {
			t.Fatalf("soft failure during network down changed state: before=%+v after=%+v", before, state)
		}
	})

	t.Run("soft while network good", func(t *testing.T) {
		state := base
		state.SoftFails = 0
		applyFailureV2Config(&state, now, FailTimeout, NetSnapshot{State: NetGood}, false, cfg)
		if state.SoftFails != 1 {
			t.Fatalf("SoftFails = %d, want %d", state.SoftFails, base.SoftFails+1)
		}
		if state.Score != base.Score || state.Penalty != base.Penalty {
			t.Fatalf("soft failure must not change reputation: score %.2f/%.2f penalty %.2f/%.2f", state.Score, base.Score, state.Penalty, base.Penalty)
		}
		minBan := now.Add(-time.Millisecond).Add(4*time.Minute + 15*time.Second)
		maxBan := now.Add(6*time.Minute + time.Second)
		if state.ReviveAt.Before(minBan) || state.ReviveAt.After(maxBan) {
			t.Fatalf("unexpected soft ban %s", state.ReviveAt.Sub(now))
		}
	})

	t.Run("hard", func(t *testing.T) {
		state := base
		applyFailureV2Config(&state, now, FailRefused, NetSnapshot{State: NetGood}, false, cfg)
		if !state.IsDead || state.Suspect {
			t.Fatalf("hard failure must ban proxy: %+v", state)
		}
		if state.SoftFails != 0 {
			t.Fatalf("hard failure must reset SoftFails, got %d", state.SoftFails)
		}
		if state.Penalty <= base.Penalty || state.Score >= base.Score {
			t.Fatalf("hard failure must change reputation: before %.2f/%.2f after %.2f/%.2f", base.Penalty, base.Score, state.Penalty, state.Score)
		}
		if !state.ReviveAt.After(now) {
			t.Fatal("hard failure must schedule a revival")
		}
	})
}

func TestLivenessFailurePromotesToSuspect(t *testing.T) {
	p := NewPool()
	now := time.Now().UTC()
	state := ProxyState{
		URL:           "http://127.0.0.1:8080",
		Score:         8,
		Penalty:       1,
		LastCheckedAt: now.Add(-time.Minute),
	}
	beforeScore, beforePenalty := state.Score, state.Penalty
	if !p.applyFailureResult(&state, now, FailRefused, NetSnapshot{State: NetGood}, false, "liveness") {
		t.Fatal("liveness failure should be accounted")
	}
	if state.IsDead || !state.Suspect {
		t.Fatalf("alive liveness failure must promote to suspect: %+v", state)
	}
	if state.Score != beforeScore || state.Penalty != beforePenalty {
		t.Fatalf("suspect transition must not change reputation: %.2f/%.2f", state.Score, state.Penalty)
	}
	wantDue := now.Add(10 * time.Second)
	if state.ReviveAt.Before(wantDue.Add(-time.Millisecond)) || state.ReviveAt.After(wantDue.Add(time.Millisecond)) {
		t.Fatalf("suspect retry = %s, want %s", state.ReviveAt.Sub(now), wantDue.Sub(now))
	}
}

func TestSuspect(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.10:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.10", Port: 8080, Score: 9, Penalty: 0.5})
	if !p.Suspect(url, FailEOF) {
		t.Fatal("known alive proxy must become suspect")
	}
	state, ok := p.cache.Get(url)
	if !ok || !state.Suspect || state.IsDead {
		t.Fatalf("unexpected suspect state: %+v/%v", state, ok)
	}
	if state.Score != 9 || state.Penalty != 0.5 {
		t.Fatalf("Suspect must preserve reputation: %.2f/%.2f", state.Score, state.Penalty)
	}
	if len(p.ListProxies(ProxyFilter{})) != 0 {
		t.Fatal("suspect proxy must be excluded from ListProxies")
	}
	if p.Suspect("http://192.0.2.99:8080", FailEOF) {
		t.Fatal("unknown proxy must return false")
	}
	state.Suspect = false
	state.IsDead = true
	p.cache.Set(state)
	if p.Suspect(url, FailEOF) {
		t.Fatal("dead proxy must not be marked suspect")
	}
}
