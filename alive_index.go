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
	// onAlive is called without the lock held after a proxy was stored as alive
	// or the whole index was replaced.
	onAlive func()
}

func newAliveIndex() *aliveIndex {
	return &aliveIndex{items: make(map[string]ProxyInfo), dirty: true}
}

func (i *aliveIndex) upsert(state ProxyState) {
	i.mu.Lock()
	alive := i.upsertLocked(state)
	hook := i.onAlive
	i.mu.Unlock()
	if alive && hook != nil {
		hook()
	}
}

func (i *aliveIndex) upsertLocked(state ProxyState) bool {
	info, alive := proxyInfoFromState(state), !state.IsDead && !state.Suspect
	if alive {
		i.items[state.URL] = info
	} else {
		delete(i.items, state.URL)
	}
	if i.builtAt.IsZero() {
		i.dirty = true
		return alive
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
		return alive
	}
	if alive {
		i.cached = append(i.cached, info)
		sort.Slice(i.cached, func(a, b int) bool { return i.cached[a].Latency < i.cached[b].Latency })
	}
	i.dirty = false
	return alive
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
	i.replaceLocked(states)
	hook := i.onAlive
	i.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (i *aliveIndex) replaceLocked(states []ProxyState) {
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
	i.ensureCachedLocked()
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

func (i *aliveIndex) ensureCachedLocked() {
	if !i.dirty {
		return
	}
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

// match returns up to max alive proxies in latency order whose location is in
// countries (empty = any) and whose URL is not in exclude.
func (i *aliveIndex) match(countries []string, exclude map[string]struct{}, max int) []ProxyInfo {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.ensureCachedLocked()
	out := make([]ProxyInfo, 0, min(max, len(i.cached)))
	for _, item := range i.cached {
		if len(out) >= max {
			break
		}
		if _, skip := exclude[item.URL]; skip {
			continue
		}
		if len(countries) > 0 && !locationIn(countries, item.Location) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func locationIn(countries []string, location string) bool {
	for _, c := range countries {
		if strings.EqualFold(c, location) {
			return true
		}
	}
	return false
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
