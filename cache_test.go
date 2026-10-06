package proxypool

import (
	"testing"
	"time"
)

func TestMemoryCacheClear(t *testing.T) {
	c := newMemoryCache()
	c.Set(ProxyState{URL: "http://1.2.3.4:8080"})
	if len(c.All()) != 1 {
		t.Fatal("expected 1 entry before clear")
	}
	c.Clear()
	if len(c.All()) != 0 {
		t.Fatal("clear must empty the cache")
	}
	if _, ok := c.Get("http://1.2.3.4:8080"); ok {
		t.Fatal("cleared entry must miss")
	}
}

func TestUpdateMetadataNoops(t *testing.T) {
	p := NewPool()
	// Must not panic on unknown URLs or nil updates.
	p.UpdateMetadata("http://127.0.0.1:9", func(m map[string]string) { m["k"] = "v" })
	p.UpdateMetadata("", nil)
	p.cache.Set(ProxyState{URL: "http://127.0.0.1:9"})
	p.UpdateMetadata("http://127.0.0.1:9", nil)
	if got := p.GetMetadata("http://127.0.0.1:9"); len(got) != 0 {
		t.Fatalf("nil update must leave metadata empty, got %v", got)
	}
}

func TestGetMetadataDetached(t *testing.T) {
	p := NewPool()
	p.cache.Set(ProxyState{URL: "http://127.0.0.1:9", Metadata: map[string]string{"k": "v"}})
	got := p.GetMetadata("http://127.0.0.1:9")
	got["k"] = "mutated"
	got["evil"] = "injected"
	fresh := p.GetMetadata("http://127.0.0.1:9")
	if fresh["k"] != "v" || len(fresh) != 1 {
		t.Fatalf("returned map must be detached, got %v", fresh)
	}
	if got := p.GetMetadata("http://unknown.invalid:9"); len(got) != 0 {
		t.Fatalf("unknown URL must yield empty map, got %v", got)
	}
}

type countingCache struct {
	*memoryCache
	sets    int
	deletes int
}

func newCountingCache() *countingCache        { return &countingCache{memoryCache: newMemoryCache()} }
func (c *countingCache) Set(state ProxyState) { c.sets++; c.memoryCache.Set(state) }
func (c *countingCache) Delete(url string)    { c.deletes++; c.memoryCache.Delete(url) }

func TestIngestNewURLStaysOutOfCacheUntilCheck(t *testing.T) {
	p := NewPool()
	p.RegisterProxySource(stubPoolSource{urls: []string{"http://127.0.0.1:1"}, name: "feed"})
	p.ingestOnce(time.Now().UTC())
	if _, ok := p.cache.Get("http://127.0.0.1:1"); ok {
		t.Fatal("new candidate must not be stored before first check")
	}
	p.sched.mu.Lock()
	defer p.sched.mu.Unlock()
	if len(p.sched.candQ) != 1 || p.sched.candQ[0].Source != "feed" {
		t.Fatalf("candidate queue mismatch: %+v", p.sched.candQ)
	}
}

func TestIngestKnownUpdatesSourceTimestampWithoutFrequentWrite(t *testing.T) {
	cache := newCountingCache()
	p := NewPool()
	p.RegisterCacheSource(cache)
	old := time.Now().UTC().Add(-10 * time.Minute)
	cache.Set(ProxyState{URL: "http://127.0.0.1:2", Source: "feed", LastCheckedAt: old, LastSeenInSource: old})
	baseline := cache.sets
	p.RegisterProxySource(stubPoolSource{urls: []string{"http://127.0.0.1:2"}, name: "feed"})
	p.ingestOnce(time.Now().UTC())
	if cache.sets != baseline {
		t.Fatalf("known proxy seen within one hour should stay in memory, sets=%d baseline=%d", cache.sets, baseline)
	}
}

type stubPoolSource struct {
	urls []string
	name string
}

func (s stubPoolSource) Name() string        { return s.name }
func (s stubPoolSource) FetchList() []string { return append([]string(nil), s.urls...) }
