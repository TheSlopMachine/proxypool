package proxypool

import (
	"testing"
	"time"
)

func TestPopCandidatePrefersHintedCountryAndSkipsConsumed(t *testing.T) {
	p := NewPool()
	add := func(url, country string) {
		item := queuedCandidate{URL: url, Source: "feed", Country: country}
		p.sched.candQ = append(p.sched.candQ, item)
		p.sched.queued[url] = struct{}{}
		if country != "" {
			p.sched.hinted[country] = append(p.sched.hinted[country], item)
		}
	}
	p.sched.mu.Lock()
	defer p.sched.mu.Unlock()
	add("http://1.1.1.1:80", "DE")
	add("http://2.2.2.2:80", "US")
	add("http://3.3.3.3:80", "")

	got, ok := p.sched.popCandidateLocked(map[string]struct{}{"US": {}})
	if !ok || got.URL != "http://2.2.2.2:80" {
		t.Fatalf("hinted US candidate must come first, got %+v/%v", got, ok)
	}
	first, _ := p.sched.popCandidateLocked(nil)
	second, _ := p.sched.popCandidateLocked(nil)
	if first.URL != "http://1.1.1.1:80" || second.URL != "http://3.3.3.3:80" {
		t.Fatalf("FIFO order broken or consumed entry returned: %q, %q", first.URL, second.URL)
	}
	if _, ok := p.sched.popCandidateLocked(nil); ok {
		t.Fatal("queue must be empty")
	}
}

func TestLaneCapsForHotAndBoost(t *testing.T) {
	hot := laneCapsFor(ModeForeground, false, true, 100, 1)
	if hot[laneForeground] != 65 || hot[laneRevival] != 20 || hot[laneBackground] != 0 {
		t.Fatalf("hot caps = %v", hot)
	}
	boosted := laneCapsFor(ModeBackground, false, false, 100, 4)
	if boosted[laneBackground] != 80 || boosted[laneLiveness] != 15 || boosted[laneRevival] != 5 {
		t.Fatalf("boosted caps = %v", boosted)
	}
	calm := laneCapsFor(ModeBackground, false, false, 100, 1)
	if calm[laneBackground] != 20 {
		t.Fatalf("calm background cap = %d, want 20", calm[laneBackground])
	}
}

func TestDemandEvaluateHotThenReplenish(t *testing.T) {
	r := newDemandRegistry()
	cfg := DefaultConfig()
	now := time.Now().UTC()
	d, err := r.acquireTarget("US", []string{"US"}, now, cfg, false, 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = d

	need := r.evaluate(now, map[string]int{}, 0)
	if !need.Hot || !need.Replenish {
		t.Fatalf("nothing matches inside the hunting window: %+v", need)
	}
	if _, ok := need.Countries["US"]; !ok {
		t.Fatalf("country missing from need: %+v", need)
	}

	need = r.evaluate(now, map[string]int{"US": 1}, 1)
	if need.Hot || !need.Replenish {
		t.Fatalf("one match below target 3 must only replenish: %+v", need)
	}

	need = r.evaluate(now, map[string]int{"US": 3}, 3)
	if need.Hot || need.Replenish || len(need.Countries) != 0 {
		t.Fatalf("target reached must be idle: %+v", need)
	}

	late := now.Add(cfg.DemandTTL + time.Second)
	need = r.evaluate(late, map[string]int{}, 0)
	if need.Hot || !need.Replenish {
		t.Fatalf("after the hunting window only replenishment remains: %+v", need)
	}
}
