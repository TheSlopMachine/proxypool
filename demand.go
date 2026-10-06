package proxypool

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// ErrInvalidCountry reports a country that is not a two-letter ISO code.
	ErrInvalidCountry = errors.New("invalid country code")
	// ErrDemandLimit reports that every demand slot is held by a demand with waiters.
	ErrDemandLimit = errors.New("too many active proxy demands")
)

const (
	maxRequireLimit = 50
	// shuffleFactor widens the candidate window before Limit proxies are drawn,
	// so concurrent callers do not all land on the lowest-latency proxy.
	shuffleFactor = 4
	shuffleMin    = 8
)

// RequireOptions describes one Require call.
type RequireOptions struct {
	// Countries is an OR list of ISO country codes. Empty accepts any country.
	Countries []string
	// Exclude lists proxy URLs that must not be returned to this caller.
	Exclude []string
	// Limit is the maximum number of distinct proxies returned. Values below 1 mean 1.
	Limit int
	// Timeout is the overall deadline of the call. Zero uses Config.RequireTimeout.
	Timeout time.Duration
}

// RequireResult is the outcome of Require. Proxies is empty when TimedOut is true.
type RequireResult struct {
	Proxies  []ProxyInfo
	TimedOut bool
}

// Require returns alive proxies matching the options. When none match, it
// registers a country demand and waits until a match becomes alive, the
// timeout elapses, or ctx is done.
//
// Cancelling ctx stops only this caller. The demand stays registered until its
// TTL expires so the pool keeps working on it. On timeout Require returns
// TimedOut=true with a nil error; the caller decides the fallback policy.
func (p *ProxyPool) Require(ctx context.Context, opts RequireOptions) (RequireResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RequireResult{}, err
	}
	countries, err := normalizeCountries(opts.Countries)
	if err != nil {
		return RequireResult{}, err
	}
	exclude := normalizeExclude(opts.Exclude)
	limit := clampInt(opts.Limit, 1, maxRequireLimit)
	cfg := p.configSnapshot()
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = cfg.RequireTimeout
	}
	key := strings.Join(countries, ",")
	started := time.Now().UTC()

	if found := p.matchAlive(countries, exclude, limit); len(found) > 0 {
		if len(countries) > 0 {
			// Keep the country demand alive so the pool maintains its stock.
			// A full registry must not fail a request that is already served.
			if d, err := p.demands.acquire(key, countries, started, cfg, false); err == nil {
				p.demands.served(d, started, len(found))
			}
		}
		return RequireResult{Proxies: found}, nil
	}

	d, err := p.demands.acquire(key, countries, started, cfg, true)
	if err != nil {
		return RequireResult{}, err
	}
	defer p.demands.release(d)

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		// Take the broadcast channel before matching so a proxy that becomes
		// alive between the match and the wait still wakes this loop.
		wake := p.demands.channel()
		if found := p.matchAlive(countries, exclude, limit); len(found) > 0 {
			now := time.Now().UTC()
			p.demands.served(d, now, len(found))
			p.demands.emit("demand_satisfied", demandFields(countries, map[string]string{
				"wait_ms": strconv.FormatInt(now.Sub(started).Milliseconds(), 10),
			}))
			return RequireResult{Proxies: found}, nil
		}
		select {
		case <-wake:
		case <-timer.C:
			p.demands.emit("require_timeout", demandFields(countries, map[string]string{
				"wait_ms": strconv.FormatInt(time.Since(started).Milliseconds(), 10),
			}))
			return RequireResult{TimedOut: true}, nil
		case <-ctx.Done():
			return RequireResult{}, ctx.Err()
		}
	}
}

// matchAlive returns up to limit alive proxies drawn from the lowest-latency
// window of matching proxies.
func (p *ProxyPool) matchAlive(countries []string, exclude map[string]struct{}, limit int) []ProxyInfo {
	window := max(limit*shuffleFactor, shuffleMin)
	matches := p.index.match(countries, exclude, window)
	if len(matches) > 1 {
		rand.Shuffle(len(matches), func(a, b int) { matches[a], matches[b] = matches[b], matches[a] })
	}
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches
}

// normalizeCountries upper-cases, validates, deduplicates and sorts country codes.
func normalizeCountries(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(raw))
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		code := strings.ToUpper(strings.TrimSpace(item))
		if len(code) != 2 || code[0] < 'A' || code[0] > 'Z' || code[1] < 'A' || code[1] > 'Z' {
			return nil, fmt.Errorf("%w: %q", ErrInvalidCountry, item)
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out, nil
}

// normalizeExclude maps proxy URLs to their canonical form. Entries that do not
// parse are kept verbatim so a caller-side key still excludes an exact match.
func normalizeExclude(raw []string) map[string]struct{} {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if ep, err := normalizeProxyURL(item); err == nil {
			out[ep.Canonical] = struct{}{}
			continue
		}
		out[item] = struct{}{}
	}
	return out
}

// demand is one registered country demand. Waiting callers share it by key.
type demand struct {
	key             string
	countries       []string
	created         time.Time
	expiresAt       time.Time
	lastSatisfiedAt time.Time
	waiters         int
	served          int64
}

// demandRegistry tracks demands and broadcasts alive-pool changes to waiters.
type demandRegistry struct {
	mu    sync.Mutex
	items map[string]*demand
	bcast chan struct{}
	// onEvent receives registry events. It is called without mu held.
	onEvent func(PoolEvent)
}

func newDemandRegistry() *demandRegistry {
	return &demandRegistry{items: make(map[string]*demand), bcast: make(chan struct{})}
}

// notify wakes every waiter after the alive set changed.
func (r *demandRegistry) notify() {
	r.mu.Lock()
	close(r.bcast)
	r.bcast = make(chan struct{})
	r.mu.Unlock()
}

func (r *demandRegistry) channel() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bcast
}

// acquire returns the demand for key, creating it when absent, and extends its
// TTL. With waiter set, the caller is counted until release.
func (r *demandRegistry) acquire(key string, countries []string, now time.Time, cfg Config, waiter bool) (*demand, error) {
	var events []PoolEvent
	r.mu.Lock()
	events = append(events, r.purgeLocked(now)...)
	d, ok := r.items[key]
	if !ok {
		if len(r.items) >= cfg.MaxDemands {
			victim := r.evictableLocked()
			if victim == nil {
				r.mu.Unlock()
				r.emitAll(events)
				return nil, ErrDemandLimit
			}
			delete(r.items, victim.key)
			events = append(events, r.expiredEvent(victim, now, "evicted"))
		}
		d = &demand{key: key, countries: append([]string(nil), countries...), created: now}
		r.items[key] = d
		events = append(events, PoolEvent{Time: now, Kind: "demand_created", Fields: demandFields(countries, nil)})
	}
	d.expiresAt = now.Add(cfg.DemandTTL)
	if waiter {
		d.waiters++
	}
	r.mu.Unlock()
	r.emitAll(events)
	return d, nil
}

func (r *demandRegistry) release(d *demand) {
	r.mu.Lock()
	if d.waiters > 0 {
		d.waiters--
	}
	r.mu.Unlock()
}

func (r *demandRegistry) served(d *demand, now time.Time, n int) {
	r.mu.Lock()
	d.served += int64(n)
	d.lastSatisfiedAt = now
	r.mu.Unlock()
}

// snapshot returns the registered demands sorted by key after dropping expired ones.
func (r *demandRegistry) snapshot(now time.Time) []DemandStat {
	r.mu.Lock()
	events := r.purgeLocked(now)
	out := make([]DemandStat, 0, len(r.items))
	for _, d := range r.items {
		out = append(out, DemandStat{
			Countries:       append([]string(nil), d.countries...),
			Waiters:         d.waiters,
			Served:          d.served,
			CreatedAt:       d.created,
			ExpiresAt:       d.expiresAt,
			LastSatisfiedAt: d.lastSatisfiedAt,
		})
	}
	r.mu.Unlock()
	r.emitAll(events)
	sort.Slice(out, func(i, j int) bool {
		return strings.Join(out[i].Countries, ",") < strings.Join(out[j].Countries, ",")
	})
	return out
}

// purgeLocked removes expired demands that have no waiters.
func (r *demandRegistry) purgeLocked(now time.Time) []PoolEvent {
	var events []PoolEvent
	for key, d := range r.items {
		if d.waiters == 0 && !d.expiresAt.After(now) {
			delete(r.items, key)
			events = append(events, r.expiredEvent(d, now, "ttl"))
		}
	}
	return events
}

// evictableLocked returns the waiter-free demand that expires first, or nil.
func (r *demandRegistry) evictableLocked() *demand {
	var victim *demand
	for _, d := range r.items {
		if d.waiters > 0 {
			continue
		}
		if victim == nil || d.expiresAt.Before(victim.expiresAt) {
			victim = d
		}
	}
	return victim
}

func (r *demandRegistry) expiredEvent(d *demand, now time.Time, reason string) PoolEvent {
	return PoolEvent{Time: now, Kind: "demand_expired", Fields: demandFields(d.countries, map[string]string{
		"reason": reason,
		"served": strconv.FormatInt(d.served, 10),
	})}
}

func (r *demandRegistry) emit(kind string, fields map[string]string) {
	r.emitAll([]PoolEvent{{Time: time.Now().UTC(), Kind: kind, Fields: fields}})
}

func (r *demandRegistry) emitAll(events []PoolEvent) {
	if r.onEvent == nil {
		return
	}
	for _, event := range events {
		r.onEvent(event)
	}
}

func demandFields(countries []string, extra map[string]string) map[string]string {
	fields := map[string]string{"countries": "any"}
	if len(countries) > 0 {
		fields["countries"] = strings.Join(countries, ",")
	}
	for k, v := range extra {
		fields[k] = v
	}
	return fields
}
