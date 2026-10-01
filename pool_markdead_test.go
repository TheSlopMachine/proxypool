package proxypool

import (
	"testing"
	"time"
)

func TestMarkDeadUnknown(t *testing.T) {
	p := NewPool()
	if p.MarkDead("", "dns_resolution") {
		t.Fatal("empty URL must return false")
	}
	if p.MarkDead("http://127.0.0.1:8080", "dns_resolution") {
		t.Fatal("unknown URL must return false")
	}
}

func TestMarkDeadLeavesScoringUntouched(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.1:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.1", Port: 8080, Score: 9.5, Penalty: 0.25})
	if !p.MarkDead(url, "tls_certificate_verification") {
		t.Fatal("known URL must return true")
	}
	state, ok := p.cache.Get(url)
	if !ok {
		t.Fatal("marked proxy must remain cached")
	}
	if !state.IsDead {
		t.Fatal("marked proxy must be dead")
	}
	if state.Score != 9.5 || state.Penalty != 0.25 {
		t.Fatalf("Score/Penalty must stay probe-owned, got %.2f/%.2f", state.Score, state.Penalty)
	}
	if state.ReviveAt.Before(time.Now().UTC().Add(14 * time.Minute)) {
		t.Fatal("first mark must exile well past the probe minimum")
	}
	if got := p.GetMetadata(url)["manual_dead_reason"]; got != "tls_certificate_verification" {
		t.Fatalf("reason must be readable, got %q", got)
	}
}

func TestMarkDeadEscalatesAndCaps(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.2:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.2", Port: 8080})
	var prev time.Time
	for i := 0; i < 5; i++ {
		if !p.MarkDead(url, "connection_refused") {
			t.Fatal("mark must return true")
		}
		state, _ := p.cache.Get(url)
		if !state.ReviveAt.After(prev) {
			t.Fatalf("ban must grow on repeat marks, round %d", i)
		}
		prev = state.ReviveAt
	}
	// Sixth mark hits the cap; same-tick bans may tie, never exceed.
	p.MarkDead(url, "connection_refused")
	state, _ := p.cache.Get(url)
	if state.ReviveAt.Before(prev) {
		t.Fatal("capped ban must not move earlier")
	}
	state, _ = p.cache.Get(url)
	if state.ReviveAt.Sub(time.Now().UTC()) > markDeadMaxBan+time.Minute {
		t.Fatal("ban must respect the 4h cap")
	}
}

func TestMarkDeadFlapNeverForgivenIdleForgiven(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.3:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.3", Port: 8080})
	p.MarkDead(url, "dns_resolution")
	p.MarkDead(url, "dns_resolution")
	state, _ := p.cache.Get(url)
	if got := state.Metadata[markCountKey]; got != "2" {
		t.Fatalf("flapping marks must accumulate, got %q", got)
	}
	stale, _ := p.cache.Get(url)
	stale.Metadata[markLastAtKey] = time.Now().UTC().Add(-25 * time.Hour).Format(time.RFC3339)
	p.cache.Set(stale)
	p.MarkDead(url, "dns_resolution")
	state, _ = p.cache.Get(url)
	if got := state.Metadata[markCountKey]; got != "1" {
		t.Fatalf("day-idle mark must restart at 1, got %q", got)
	}
}

func TestMergeManualMarkPreservesNewerMark(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.4:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.4", Port: 8080})
	snapshot, _ := p.cache.Get(url)
	p.MarkDead(url, "tls_certificate_verification")
	current, _ := p.cache.Get(url)
	merged := mergeManualMark(current, snapshot)
	if !merged.IsDead {
		t.Fatal("newer manual mark must survive pipeline write-back")
	}
	if !merged.ReviveAt.Equal(current.ReviveAt) {
		t.Fatal("merged ReviveAt must carry the manual ban")
	}
	if markCount(merged.Metadata) != 1 {
		t.Fatal("merged metadata must carry the manual count")
	}
}

func TestMergeManualMarkKeepsPipelineVerdict(t *testing.T) {
	snapshot := ProxyState{URL: "http://192.0.2.5:8080"}
	updated := ProxyState{URL: "http://192.0.2.5:8080", Score: 3.0}
	merged := mergeManualMark(snapshot, updated)
	if merged.IsDead || merged.Score != 3.0 {
		t.Fatal("no intervening mark means the pipeline verdict stands")
	}
}

func TestMarkDeadHidesFromList(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.6:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.6", Port: 8080})
	if len(p.ListProxies(ProxyFilter{})) != 1 {
		t.Fatal("expected 1 listed proxy before mark")
	}
	if !p.MarkDead(url, "dns_resolution") {
		t.Fatal("mark must return true")
	}
	if len(p.ListProxies(ProxyFilter{})) != 0 {
		t.Fatal("marked proxy must leave ListProxies")
	}
}
