package proxypool

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type aliveIndex struct {
	mu      sync.RWMutex
	items   map[string]ProxyInfo
	cached  []ProxyInfo
	dirty   bool
	builtAt time.Time
}

func newAliveIndex() *aliveIndex {
	return &aliveIndex{items: make(map[string]ProxyInfo), dirty: true}
}

func (i *aliveIndex) upsert(state ProxyState) {
	i.mu.Lock()
	defer i.mu.Unlock()
	info, alive := proxyInfoFromState(state), !state.IsDead && !state.Suspect
	if alive {
		i.items[state.URL] = info
	} else {
		delete(i.items, state.URL)
	}
	if i.builtAt.IsZero() {
		i.dirty = true
		return
	}
	for n := range i.cached {
		if i.cached[n].URL != state.URL {
			continue
		}
		if !alive {
			i.cached = append(i.cached[:n], i.cached[n+1:]...)
		} else {
			i.cached[n] = info
		}
		sort.Slice(i.cached, func(a, b int) bool { return i.cached[a].Latency < i.cached[b].Latency })
		i.dirty = false
		return
	}
	if alive {
		i.cached = append(i.cached, info)
		sort.Slice(i.cached, func(a, b int) bool { return i.cached[a].Latency < i.cached[b].Latency })
	}
	i.dirty = false
}

func (i *aliveIndex) delete(url string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.items, url)
	if i.builtAt.IsZero() {
		i.dirty = true
		return
	}
	for n := range i.cached {
		if i.cached[n].URL == url {
			i.cached = append(i.cached[:n], i.cached[n+1:]...)
			break
		}
	}
	i.dirty = false
}

func (i *aliveIndex) replace(states []ProxyState) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.items = make(map[string]ProxyInfo, len(states))
	for _, state := range states {
		if !state.IsDead && !state.Suspect {
			i.items[state.URL] = proxyInfoFromState(state)
		}
	}
	i.cached = nil
	i.dirty = true
	i.builtAt = time.Time{}
}

func (i *aliveIndex) list(filter ProxyFilter) []ProxyInfo {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.dirty {
		i.cached = i.cached[:0]
		for _, info := range i.items {
			i.cached = append(i.cached, info)
		}
		sort.Slice(i.cached, func(a, b int) bool {
			return i.cached[a].Latency < i.cached[b].Latency
		})
		i.builtAt = time.Now()
		i.dirty = false
	}
	matches := make([]ProxyInfo, 0, len(i.cached))
	for _, item := range i.cached {
		if filter.Location != nil && !strings.EqualFold(item.Location, *filter.Location) {
			continue
		}
		if filter.MaxLatency != nil && item.Latency > *filter.MaxLatency {
			continue
		}
		if filter.MinScore != nil && item.Score < *filter.MinScore {
			continue
		}
		matches = append(matches, item)
	}
	if filter.Limit != nil && *filter.Limit > 0 && len(matches) > *filter.Limit {
		matches = matches[:*filter.Limit]
	}
	return append([]ProxyInfo(nil), matches...)
}

func proxyInfoFromState(item ProxyState) ProxyInfo {
	return ProxyInfo{
		URL:         item.URL,
		IP:          item.IP,
		Port:        item.Port,
		Location:    item.Location,
		Latency:     item.Latency,
		Score:       math.Round(item.Score*100) / 100,
		LastChecked: item.LastCheckedAt,
	}
}
