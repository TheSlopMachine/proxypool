package proxypool

import (
	"testing"
	"time"
)

func TestAliveIndexFiltersSuspectAndBanned(t *testing.T) {
	idx := newAliveIndex()
	now := time.Now().UTC()
	idx.upsert(ProxyState{URL: "http://1.1.1.1:80", Location: "US", Latency: 30 * time.Millisecond, Score: 8, LastCheckedAt: now})
	idx.upsert(ProxyState{URL: "http://2.2.2.2:80", Suspect: true})
	idx.upsert(ProxyState{URL: "http://3.3.3.3:80", IsDead: true})
	items := idx.list(ProxyFilter{})
	if len(items) != 1 || items[0].URL != "http://1.1.1.1:80" {
		t.Fatalf("unexpected index contents: %+v", items)
	}
}

func TestAliveIndexLazySortAndFilter(t *testing.T) {
	idx := newAliveIndex()
	idx.upsert(ProxyState{URL: "slow", Latency: 90 * time.Millisecond, Score: 7})
	idx.upsert(ProxyState{URL: "fast", Latency: 20 * time.Millisecond, Score: 9})
	min := 8.0
	items := idx.list(ProxyFilter{MinScore: &min})
	if len(items) != 1 || items[0].URL != "fast" {
		t.Fatalf("unexpected filtered index: %+v", items)
	}
	if got := items[0].Score; got != 9 {
		t.Fatalf("score rounding changed: %v", got)
	}
}
