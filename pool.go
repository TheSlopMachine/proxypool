package proxypool

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ProxyPool coordinates proxy discovery, lifecycle health, and consumer queries.
type ProxyPool struct {
	mu          sync.RWMutex
	cache       CacheSource
	sources     []ProxySource
	reporter    RefreshReporter
	timeouts    TimeoutConfig
	concurrency int
}

// NewPool initializes a pool with stock-Windows-safe defaults
// (DefaultConcurrency workers, DefaultTimeoutConfig budgets) and default
// in-memory storage with no-op reporting. See transport.go for why every
// network phase must be deadline-bounded.
func NewPool() *ProxyPool {
	pool := &ProxyPool{
		sources:     make([]ProxySource, 0),
		timeouts:    DefaultTimeoutConfig(),
		concurrency: DefaultConcurrency,
		reporter:    &noopReporter{},
	}
	pool.RegisterCacheSource(newMemoryCache())
	return pool
}

// RegisterCacheSource switches the storage backend and automatically migrates
// all existing proxy states from the old cache to the new one.
func (p *ProxyPool) RegisterCacheSource(newCache CacheSource) {
	if newCache == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cache != nil {
		existing := p.cache.All()
		for _, state := range existing {
			newCache.Set(state)
		}
	}
	p.cache = newCache
}

// RegisterProxySource adds an upstream proxy feed provider.
func (p *ProxyPool) RegisterProxySource(source ProxySource) {
	if source == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.sources = append(p.sources, source)
}

// RegisterReporter registers a telemetry metrics handler.
func (p *ProxyPool) RegisterReporter(reporter RefreshReporter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if reporter == nil {
		p.reporter = &noopReporter{}
		return
	}
	p.reporter = reporter
}

// SetConcurrency configures the maximum concurrent verification workers.
func (p *ProxyPool) SetConcurrency(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n > 0 {
		p.concurrency = n
	}
}

// SetTimeout replaces the network budgets for handshakes and latency
// probes. A zero value resolves to all defaults via TimeoutConfig.Resolve;
// to change one phase, pass the other through unchanged.
func (p *ProxyPool) SetTimeout(c TimeoutConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.timeouts = c.Resolve()
}

// Refresh performs a verification cycle:
// 1. Fetches raw URLs from all registered ProxySources.
// 2. Ingests and canonicalizes URLs into the cache.
// 3. Filters candidates (new + alive + due revivals).
// 4. Runs concurrent verification (handshake + dual-probe).
// 5. Updates cache states and broadcasts telemetry reports.
func (p *ProxyPool) Refresh() {
	startTime := time.Now()
	now := startTime.UTC()

	p.mu.RLock()
	cache := p.cache
	sources := append([]ProxySource(nil), p.sources...)
	reporter := p.reporter
	concurrency := p.concurrency
	timeouts := p.timeouts.Resolve()
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	p.mu.RUnlock()

	// 1. Ingestion: Fetch URLs from sources & normalize
	totalNew := 0
	for _, src := range sources {
		rawURLs := src.FetchList()
		for _, raw := range rawURLs {
			ep, err := normalizeProxyURL(raw)
			if err != nil {
				continue
			}
			if _, exists := cache.Get(ep.Canonical); !exists {
				totalNew++
				cache.Set(ProxyState{
					URL:           ep.Canonical,
					IP:            ep.Host,
					Port:          ep.Port,
					IsDead:        false,
					Penalty:       0.0,
					Score:         0.0,
					LastCheckedAt: time.Time{},
				})
			}
		}
	}

	// 2. Candidate Selection
	allStates := cache.All()
	var candidates []ProxyState
	skippedCount := 0
	revivedDueCount := 0

	for _, item := range allStates {
		if !item.IsDead {
			candidates = append(candidates, item)
		} else {
			if !item.ReviveAt.IsZero() && (now.After(item.ReviveAt) || now.Equal(item.ReviveAt)) {
				revivedDueCount++
				candidates = append(candidates, item)
			} else {
				skippedCount++
			}
		}
	}

	// 3. Execution & Verification Pipeline
	// startTime (raw time.Now) feeds Elapsed so progress reports measure the
	// full cycle; now (UTC) stays the record timestamp.
	outcomes := executePipeline(candidates, PipelineConfig{
		Concurrency: concurrency,
		Timeouts:    timeouts,
		Now:         now,
		CycleStart:  startTime,
		Reporter:    reporter,
	})

	// 4. Persist Updates & Stream Proxy Reports
	survivedCount := 0
	diedCount := 0

	for _, outcome := range outcomes {
		st := outcome.State
		// The pipeline ran on a snapshot; a manual mark landing
		// mid-cycle must survive this write-back.
		if current, ok := cache.Get(st.URL); ok {
			st = mergeManualMark(current, st)
		}
		cache.Set(st)
		reporter.ReportProxy(outcome.Report)
		if !st.IsDead {
			survivedCount++
		}
		if outcome.Report.Died {
			diedCount++
		}
	}

	// 5. Compute Cycle Telemetry Metrics across the Active Pool in one pass.
	currentAlive := cache.All()
	var activeLatencies []time.Duration

	aliveCount := 0
	var scoreSum, penaltySum, maxScore, maxPenalty float64
	for _, item := range currentAlive {
		if item.IsDead {
			continue
		}
		aliveCount++
		activeLatencies = append(activeLatencies, item.Latency)
		scoreSum += item.Score
		penaltySum += item.Penalty
		if item.Score > maxScore {
			maxScore = item.Score
		}
		if item.Penalty > maxPenalty {
			maxPenalty = item.Penalty
		}
	}

	var minLat, maxLat, medLat time.Duration
	if len(activeLatencies) > 0 {
		sort.Slice(activeLatencies, func(i, j int) bool { return activeLatencies[i] < activeLatencies[j] })
		minLat = activeLatencies[0]
		maxLat = activeLatencies[len(activeLatencies)-1]
		medLat = medianDuration(activeLatencies)
	}

	meanScore, meanPenalty := 0.0, 0.0
	if aliveCount > 0 {
		meanScore = scoreSum / float64(aliveCount)
		meanPenalty = penaltySum / float64(aliveCount)
	}

	// 6. Broadcast Aggregated Cycle Report
	reporter.Report(RefreshReport{
		Timestamp:            startTime,
		Duration:             time.Since(startTime),
		ProxiesTotal:         len(currentAlive),
		ProxiesTotalNew:      totalNew,
		ProxiesTotalSkipped:  skippedCount,
		ProxiesTotalRevived:  revivedDueCount,
		ProxiesTotalProbed:   len(candidates),
		ProxiesTotalSurvived: survivedCount,
		ProxiesTotalDied:     diedCount,
		ProxiesTotalAlive:    aliveCount,
		LatencyMinimum:       minLat,
		LatencyMaximum:       maxLat,
		LatencyMedian:        medLat,
		ScoreMaximum:         maxScore,
		ScoreAverage:         meanScore,
		PenaltyMaximum:       maxPenalty,
		PenaltyAverage:       meanPenalty,
	})
}

// ListProxies returns active proxies matching the provided filter criteria,
// sorted by lowest latency first.
func (p *ProxyPool) ListProxies(filter ProxyFilter) []ProxyInfo {
	p.mu.RLock()
	cache := p.cache
	p.mu.RUnlock()

	all := cache.All()
	matches := make([]ProxyInfo, 0, len(all))

	for _, item := range all {
		if item.IsDead {
			continue
		}

		if filter.Location != nil && !strings.EqualFold(item.Location, *filter.Location) {
			continue
		}

		if filter.MaxLatency != nil && item.Latency > *filter.MaxLatency {
			continue
		}

		if filter.MinScore != nil && item.Score < *filter.MinScore {
			continue
		}

		matches = append(matches, ProxyInfo{
			URL:         item.URL,
			IP:          item.IP,
			Port:        item.Port,
			Location:    item.Location,
			Latency:     item.Latency,
			Score:       math.Round(item.Score*100) / 100,
			LastChecked: item.LastCheckedAt,
		})
	}

	// Sort ascending by response latency
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Latency < matches[j].Latency
	})

	// Apply Limit (KISS)
	if filter.Limit != nil && *filter.Limit > 0 && len(matches) > *filter.Limit {
		matches = matches[:*filter.Limit]
	}

	return matches
}

// Manual exclusion schedule for MarkDead. Bans escalate per repeat mark
// and stay below the feed refresh cycle; forgiveness outlives the maximum
// flap period so re-offending proxies never qualify.
const (
	markDeadBaseBan      = 15 * time.Minute
	markDeadMaxBan       = 4 * time.Hour
	markDeadForgiveAfter = 24 * time.Hour
)

// Metadata keys carrying manual exclusion memory. Score and Penalty stay
// probe-only; these keys live on a separate channel the probe never heals.
const (
	markCountKey  = "manual_dead_count"
	markLastAtKey = "manual_dead_last_at"
	markReasonKey = "manual_dead_reason"
)

// MarkDead excludes the proxy with the given canonical URL until an
// escalating ban expires. It records an upstream-observed fault the probe
// cannot see, so it never touches Score or Penalty. Unknown or empty URLs
// return false. Repeat marks extend the ban; a full idle day forgives.
func (p *ProxyPool) MarkDead(url, reason string) bool {
	if url == "" {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cache == nil {
		return false
	}
	state, ok := p.cache.Get(url)
	if !ok {
		return false
	}
	now := time.Now().UTC()
	count := markCount(state.Metadata)
	if last := markLastAt(state.Metadata); !last.IsZero() && now.Sub(last) > markDeadForgiveAfter {
		count = 0
	}
	count++
	ban := markDeadBaseBan
	for i := 1; i < count && ban < markDeadMaxBan; i++ {
		ban *= 2
		if ban > markDeadMaxBan || ban <= 0 {
			ban = markDeadMaxBan
			break
		}
	}
	if state.Metadata == nil {
		state.Metadata = make(map[string]string)
	}
	state.Metadata[markCountKey] = strconv.Itoa(count)
	state.Metadata[markLastAtKey] = now.Format(time.RFC3339)
	if reason != "" {
		state.Metadata[markReasonKey] = reason
	}
	state.IsDead = true
	if revive := now.Add(ban); revive.After(state.ReviveAt) {
		state.ReviveAt = revive
	}
	state.LastCheckedAt = now
	p.cache.Set(state)
	if p.reporter != nil {
		p.reporter.ReportProxy(ProxyReport{
			Timestamp: now,
			URL:       state.URL,
			Location:  state.Location,
			IsDead:    state.IsDead,
			Score:     state.Score,
			Penalty:   state.Penalty,
			ReviveAt:  state.ReviveAt,
			Latency:   state.Latency,
		})
	}
	return true
}

// mergeManualMark preserves a newer manual exclusion over pipeline output.
// Refresh snapshots candidates before probing; a mark landing mid-cycle
// must survive the stale write-back. Equal counts mean no intervening
// mark, so the pipeline verdict stands.
func mergeManualMark(current, updated ProxyState) ProxyState {
	if markCount(current.Metadata) <= markCount(updated.Metadata) {
		return updated
	}
	updated.IsDead = true
	if current.ReviveAt.After(updated.ReviveAt) {
		updated.ReviveAt = current.ReviveAt
	}
	if !current.LastCheckedAt.IsZero() && current.LastCheckedAt.After(updated.LastCheckedAt) {
		updated.LastCheckedAt = current.LastCheckedAt
	}
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]string)
	}
	for _, k := range []string{markCountKey, markLastAtKey, markReasonKey} {
		if v, ok := current.Metadata[k]; ok {
			updated.Metadata[k] = v
		}
	}
	return updated
}

func markCount(metadata map[string]string) int {
	n, err := strconv.Atoi(metadata[markCountKey])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func markLastAt(metadata map[string]string) time.Time {
	t, err := time.Parse(time.RFC3339, metadata[markLastAtKey])
	if err != nil {
		return time.Time{}
	}
	return t
}

// UpdateMetadata runs update against the custom metadata map for the given
// proxy URL (exact match, no normalization). Initializes the map if nil.
// No-op if the proxy is unknown or update is nil.
func (p *ProxyPool) UpdateMetadata(url string, update func(map[string]string)) {
	if update == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	state, ok := p.cache.Get(url)
	if !ok {
		return
	}
	if state.Metadata == nil {
		state.Metadata = make(map[string]string)
	}
	update(state.Metadata)
	p.cache.Set(state)
}

// GetMetadata returns a copy of the custom metadata for the given proxy URL
// (exact match, no normalization). Returns a new empty map if the proxy is
// unknown or has no metadata. The returned map is detached from the pool.
func (p *ProxyPool) GetMetadata(url string) map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	state, ok := p.cache.Get(url)
	if !ok || state.Metadata == nil {
		return make(map[string]string)
	}
	out := make(map[string]string, len(state.Metadata))
	for k, v := range state.Metadata {
		out[k] = v
	}
	return out
}
