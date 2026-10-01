package proxypool

import (
	"testing"
	"time"
)

func TestPenalizeProxyUnknown(t *testing.T) {
	p := NewPool()
	if p.PenalizeProxy("", "dns_resolution") {
		t.Fatal("empty URL must return false")
	}
	if p.PenalizeProxy("http://127.0.0.1:8080", "dns_resolution") {
		t.Fatal("unknown URL must return false")
	}
}

func TestPenalizeProxyAppliesFailure(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.1:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.1", Port: 8080, Score: 5.0})
	if !p.PenalizeProxy(url, "tls_certificate_verification") {
		t.Fatal("known URL must return true")
	}
	state, ok := p.cache.Get(url)
	if !ok {
		t.Fatal("penalized proxy must remain cached")
	}
	if !state.IsDead {
		t.Fatal("penalized proxy must be dead")
	}
	if state.Penalty <= 0 {
		t.Fatalf("penalty must increase, got %v", state.Penalty)
	}
	if state.Score >= 5.0 {
		t.Fatalf("score must decrease, got %v", state.Score)
	}
	if state.ReviveAt.Before(time.Now().UTC()) {
		t.Fatal("ReviveAt must be in the future")
	}
	if state.Metadata["last_penalty_reason"] != "tls_certificate_verification" {
		t.Fatalf("reason must persist, got %q", state.Metadata["last_penalty_reason"])
	}
	if got := p.GetMetadata(url)["last_penalty_reason"]; got != "tls_certificate_verification" {
		t.Fatalf("reason must be readable, got %q", got)
	}
}

func TestPenalizeProxyExtendsBan(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.2:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.2", Port: 8080})
	if !p.PenalizeProxy(url, "connection_refused") {
		t.Fatal("first penalize must return true")
	}
	first, _ := p.cache.Get(url)
	if !p.PenalizeProxy(url, "connection_refused") {
		t.Fatal("second penalize must return true")
	}
	second, _ := p.cache.Get(url)
	if second.Penalty <= first.Penalty {
		t.Fatalf("penalty must grow, first %v second %v", first.Penalty, second.Penalty)
	}
	if !second.ReviveAt.After(first.ReviveAt) && !second.ReviveAt.Equal(first.ReviveAt) {
		t.Fatal("ReviveAt must not move earlier on repeat penalty")
	}
}

func TestPenalizeProxyHidesFromList(t *testing.T) {
	p := NewPool()
	url := "http://192.0.2.3:8080"
	p.cache.Set(ProxyState{URL: url, IP: "192.0.2.3", Port: 8080})
	matches := p.ListProxies(ProxyFilter{})
	if len(matches) != 1 {
		t.Fatalf("expected 1 listed proxy, got %d", len(matches))
	}
	if !p.PenalizeProxy(url, "dns_resolution") {
		t.Fatal("penalize must return true")
	}
	matches = p.ListProxies(ProxyFilter{})
	if len(matches) != 0 {
		t.Fatalf("penalized proxy must leave ListProxies, got %d", len(matches))
	}
}
