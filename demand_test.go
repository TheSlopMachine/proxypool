package proxypool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func setAlive(p *ProxyPool, url, location string) {
	p.cache.Set(ProxyState{URL: url, Location: location, LastCheckedAt: time.Now().UTC()})
}

func TestRequireFastPathMatchesCountryAndExclude(t *testing.T) {
	p := NewPool()
	setAlive(p, "http://1.1.1.1:80", "US")
	setAlive(p, "http://2.2.2.2:80", "CA")
	setAlive(p, "http://3.3.3.3:80", "DE")

	res, err := p.Require(context.Background(), RequireOptions{Countries: []string{"us", "ca"}, Limit: 5})
	if err != nil || res.TimedOut {
		t.Fatalf("Require = %+v, %v", res, err)
	}
	if len(res.Proxies) != 2 {
		t.Fatalf("got %d proxies, want 2 (US and CA only)", len(res.Proxies))
	}
	for _, info := range res.Proxies {
		if info.Location == "DE" {
			t.Fatalf("returned proxy outside the OR list: %+v", info)
		}
	}

	res, err = p.Require(context.Background(), RequireOptions{
		Countries: []string{"US", "CA"},
		Exclude:   []string{"http://1.1.1.1:80"},
		Limit:     5,
	})
	if err != nil || len(res.Proxies) != 1 || res.Proxies[0].URL != "http://2.2.2.2:80" {
		t.Fatalf("exclude not honored: %+v, %v", res, err)
	}
}

func TestRequireEmptyCountriesAcceptsAny(t *testing.T) {
	p := NewPool()
	setAlive(p, "http://1.1.1.1:80", "BR")
	res, err := p.Require(context.Background(), RequireOptions{})
	if err != nil || len(res.Proxies) != 1 {
		t.Fatalf("Require = %+v, %v", res, err)
	}
}

func TestRequireLimitCapsResult(t *testing.T) {
	p := NewPool()
	for _, u := range []string{"http://1.1.1.1:80", "http://2.2.2.2:80", "http://3.3.3.3:80"} {
		setAlive(p, u, "US")
	}
	res, err := p.Require(context.Background(), RequireOptions{Countries: []string{"US"}, Limit: 2})
	if err != nil || len(res.Proxies) != 2 {
		t.Fatalf("Require = %+v, %v", res, err)
	}
}

func TestRequireRejectsInvalidCountry(t *testing.T) {
	p := NewPool()
	for _, bad := range []string{"USA", "U", "1A", ""} {
		_, err := p.Require(context.Background(), RequireOptions{Countries: []string{bad}})
		if !errors.Is(err, ErrInvalidCountry) {
			t.Fatalf("country %q: err = %v, want ErrInvalidCountry", bad, err)
		}
	}
}

func TestRequireWaitsUntilMatchingProxyIsAlive(t *testing.T) {
	p := NewPool()
	setAlive(p, "http://9.9.9.9:80", "DE")

	done := make(chan RequireResult, 1)
	go func() {
		res, _ := p.Require(context.Background(), RequireOptions{Countries: []string{"US"}, Timeout: 5 * time.Second})
		done <- res
	}()

	waitForWaiters(t, p, 1)
	setAlive(p, "http://1.1.1.1:80", "US")

	select {
	case res := <-done:
		if res.TimedOut || len(res.Proxies) != 1 || res.Proxies[0].Location != "US" {
			t.Fatalf("result = %+v", res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter was not woken by a new alive proxy")
	}
}

func TestRequireTimeoutReturnsTimedOutAndKeepsDemand(t *testing.T) {
	p := NewPool()
	res, err := p.Require(context.Background(), RequireOptions{Countries: []string{"US"}, Timeout: 50 * time.Millisecond})
	if err != nil || !res.TimedOut || len(res.Proxies) != 0 {
		t.Fatalf("Require = %+v, %v", res, err)
	}
	demands := p.demands.snapshot(time.Now().UTC())
	if len(demands) != 1 || demands[0].Waiters != 0 {
		t.Fatalf("demand must outlive its waiter: %+v", demands)
	}
}

func TestRequireCancellationKeepsDemand(t *testing.T) {
	p := NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := p.Require(ctx, RequireOptions{Countries: []string{"US"}, Timeout: 5 * time.Second})
		errc <- err
	}()
	waitForWaiters(t, p, 1)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not release the waiter")
	}
	demands := p.demands.snapshot(time.Now().UTC())
	if len(demands) != 1 || demands[0].Waiters != 0 {
		t.Fatalf("demand must survive cancellation: %+v", demands)
	}
}

func TestRequireMergesWaitersIntoOneDemand(t *testing.T) {
	p := NewPool()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Require(context.Background(), RequireOptions{Countries: []string{"ca", "US"}, Timeout: 5 * time.Second})
		}()
	}
	waitForWaiters(t, p, 3)
	demands := p.demands.snapshot(time.Now().UTC())
	if len(demands) != 1 || demands[0].Waiters != 3 {
		t.Fatalf("want one demand with 3 waiters, got %+v", demands)
	}
	if got := demands[0].Countries; len(got) != 2 || got[0] != "CA" || got[1] != "US" {
		t.Fatalf("countries = %v, want sorted [CA US]", got)
	}
	setAlive(p, "http://1.1.1.1:80", "CA")
	wg.Wait()
}

func TestDemandExpiresAfterTTLOnly_WithoutWaiters(t *testing.T) {
	r := newDemandRegistry()
	cfg := DefaultConfig()
	now := time.Now().UTC()
	d, err := r.acquire("US", []string{"US"}, now, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	later := now.Add(cfg.DemandTTL + time.Minute)
	if got := r.snapshot(later); len(got) != 1 {
		t.Fatalf("demand with a waiter must not expire: %+v", got)
	}
	r.release(d)
	if got := r.snapshot(later); len(got) != 0 {
		t.Fatalf("demand without waiters must expire after TTL: %+v", got)
	}
}

func TestDemandLimitEvictsIdleAndRejectsWhenAllWaited(t *testing.T) {
	r := newDemandRegistry()
	cfg := DefaultConfig()
	cfg.MaxDemands = 2
	now := time.Now().UTC()
	a, _ := r.acquire("AA", []string{"AA"}, now, cfg, true)
	b, _ := r.acquire("BB", []string{"BB"}, now.Add(time.Second), cfg, true)

	if _, err := r.acquire("CC", []string{"CC"}, now.Add(2*time.Second), cfg, true); !errors.Is(err, ErrDemandLimit) {
		t.Fatalf("err = %v, want ErrDemandLimit", err)
	}
	r.release(a)
	if _, err := r.acquire("CC", []string{"CC"}, now.Add(3*time.Second), cfg, true); err != nil {
		t.Fatalf("idle demand must be evicted: %v", err)
	}
	keys := map[string]bool{}
	for _, d := range r.snapshot(now.Add(4 * time.Second)) {
		keys[d.Countries[0]] = true
	}
	if keys["AA"] || !keys["BB"] || !keys["CC"] {
		t.Fatalf("unexpected demands after eviction: %v", keys)
	}
	r.release(b)
}

func TestRequireFastPathRegistersCountryDemand(t *testing.T) {
	p := NewPool()
	setAlive(p, "http://1.1.1.1:80", "US")
	if _, err := p.Require(context.Background(), RequireOptions{Countries: []string{"US"}}); err != nil {
		t.Fatal(err)
	}
	demands := p.demands.snapshot(time.Now().UTC())
	if len(demands) != 1 || demands[0].Served != 1 {
		t.Fatalf("fast path must register and count the demand: %+v", demands)
	}
	if _, err := p.Require(context.Background(), RequireOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := p.demands.snapshot(time.Now().UTC()); len(got) != 1 {
		t.Fatalf("country-less fast path must not register a demand: %+v", got)
	}
}

func waitForWaiters(t *testing.T, p *ProxyPool, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, d := range p.demands.snapshot(time.Now().UTC()) {
			total += d.Waiters
		}
		if total >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waiters did not reach %d", n)
}
