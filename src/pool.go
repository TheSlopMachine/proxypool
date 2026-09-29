package proxypool

import (
	"math"
	"sort"
    "strings"
	"sync"
	"time"
)

// ProxyPool coordinates proxy discovery, lifecycle health, and consumer queries.
type ProxyPool struct {
	mu           sync.RWMutex
	cache        CacheSource
	sources      []ProxySource
	reporter     RefreshReporter
	checkTimeout time.Duration
	concurrency  int
}

// NewPool initializes a pool with default in-memory storage and no-op reporting.
func NewPool() *ProxyPool {
	pool := &ProxyPool{
		sources:      make([]ProxySource, 0),
		checkTimeout: 4 * time.Second,
		concurrency:  500,
		reporter:     &noopReporter{},
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

// SetTimeout configures the network timeout for handshakes and latency probes.
func (p *ProxyPool) SetTimeout(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d > 0 {
		p.checkTimeout = d
	}
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
	timeout := p.checkTimeout
	p.mu.RUnlock()

	// 1. Ingestion: Fetch URLs from sources & normalize
	totalNew := 0
	for _, src := range sources {
		rawURLs := src.FetchList()
		for _, raw := range rawURLs {
			canonical, host, port, err := normalizeProxyURL(raw)
			if err != nil {
				continue
			}
			if _, exists := cache.Get(canonical); !exists {
				totalNew++
				cache.Set(ProxyState{
					URL:           canonical,
					IP:            host,
					Port:          port,
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
	updatedStates, proxyReports := executePipeline(candidates, concurrency, timeout, now)

	// 4. Persist Updates & Stream Proxy Reports
	survivedCount := 0
	diedCount := 0

	for i, st := range updatedStates {
		cache.Set(st)
		reporter.ReportProxy(proxyReports[i])
		if !st.IsDead {
			survivedCount++
		}
		if proxyReports[i].Died {
			diedCount++
		}
	}

	// 5. Compute Cycle Telemetry Metrics across the Active Pool
	currentAlive := cache.All()
	var activeLatencies []time.Duration
	var activeScores []float64
	var activePenalties []float64

	aliveCount := 0
	for _, item := range currentAlive {
		if !item.IsDead {
			aliveCount++
			activeLatencies = append(activeLatencies, item.Latency)
			activeScores = append(activeScores, item.Score)
			activePenalties = append(activePenalties, item.Penalty)
		}
	}

	var minLat, maxLat, medLat time.Duration
	if len(activeLatencies) > 0 {
		sort.Slice(activeLatencies, func(i, j int) bool { return activeLatencies[i] < activeLatencies[j] })
		minLat = activeLatencies[0]
		maxLat = activeLatencies[len(activeLatencies)-1]
		medLat = medianDuration(activeLatencies)
	}

	maxScore := 0.0
	meanScore := 0.0
	if len(activeScores) > 0 {
		meanScore = meanFloat(activeScores)
		for _, s := range activeScores {
			if s > maxScore {
				maxScore = s
			}
		}
	}

	maxPenalty := 0.0
	meanPenalty := 0.0
	if len(activePenalties) > 0 {
		meanPenalty = meanFloat(activePenalties)
		for _, p := range activePenalties {
			if p > maxPenalty {
				maxPenalty = p
			}
		}
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