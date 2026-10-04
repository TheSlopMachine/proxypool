package proxypool

import (
	"testing"
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
