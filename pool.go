package proxypool

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ProxyPool coordinates proxy discovery, lifecycle health, scheduling, and consumer queries.
type ProxyPool struct {
	mu       sync.RWMutex
	cache    CacheSource
	sources  []ProxySource
	reporter Reporter
	timeouts TimeoutConfig
	config   Config

	limiter   *limiter
	index     *aliveIndex
	sched     *scheduler
	netHealth *netHealth
	mode      atomic.Int32
	starved   atomic.Bool
}

type scheduler struct {
	mu sync.Mutex

	candQ    []queuedCandidate
	candHead int
	queued   map[string]struct{}
	inflight map[string]struct{}

	liveHeap     scheduleHeap
	reviveHeap   scheduleHeap
	forcedQ      []string
	forced       map[string]struct{}
	laneInflight map[string]int
	lastIngestAt time.Time

	wake      chan struct{}
	ingestReq chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	start  bool
}

// NewPool initializes a pool with the configured scheduler defaults and
// default in-memory storage with no-op reporting.
func NewPool() *ProxyPool {
	cfg := DefaultConfig()
	pool := &ProxyPool{
		sources:   make([]ProxySource, 0),
		timeouts:  DefaultTimeoutConfig(),
		config:    cfg,
		reporter:  &noopReporter{},
		limiter:   newLimiter(cfg.InitialLimit),
		index:     newAliveIndex(),
		netHealth: newNetHealth(),
		sched: &scheduler{
			queued:    make(map[string]struct{}),
			inflight:  make(map[string]struct{}),
			wake:      make(chan struct{}, 1),
			ingestReq: make(chan struct{}, 1),
		},
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
		for _, state := range p.cache.All() {
			newCache.Set(state)
		}
	}
	p.index.replace(newCache.All())
	p.cache = &indexedCacheSource{inner: newCache, index: p.index}
	if p.sched != nil {
		p.sched.rebuildHeaps(newCache.All(), p.config.Resolve(), time.Now().UTC())
	}
}

// RegisterProxySource adds an upstream proxy feed provider.
func (p *ProxyPool) RegisterProxySource(source ProxySource) {
	if source == nil {
		return
	}
	p.mu.Lock()
	p.sources = append(p.sources, source)
	p.mu.Unlock()
}

// RegisterReporter registers a structured telemetry handler.
func (p *ProxyPool) RegisterReporter(reporter Reporter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if reporter == nil {
		p.reporter = &noopReporter{}
		return
	}
	p.reporter = reporter
}

// SetConfig updates scheduler configuration. Zero values resolve to defaults.
func (p *ProxyPool) SetConfig(c Config) {
	c = c.Resolve()
	p.mu.Lock()
	p.config = c
	if p.limiter == nil {
		p.limiter = newLimiter(c.InitialLimit)
	} else {
		p.limiter.SetLimit(clampInt(p.limiter.Limit(), c.MinLimit, c.MaxLimit))
	}
	p.mu.Unlock()
	p.signalWake()
}

// SetLimits sets the dynamic concurrency range and current initial limit.
func (p *ProxyPool) SetLimits(min, initial, max int) {
	p.mu.Lock()
	cfg := p.config.Resolve()
	if min <= 0 {
		min = cfg.MinLimit
	}
	if initial <= 0 {
		initial = cfg.InitialLimit
	}
	if max <= 0 {
		max = cfg.MaxLimit
	}
	if min < 1 {
		min = 1
	}
	if initial < min {
		initial = min
	}
	if max < initial {
		max = initial
	}
	cfg.MinLimit, cfg.InitialLimit, cfg.MaxLimit = min, initial, max
	p.config = cfg
	if p.limiter == nil {
		p.limiter = newLimiter(initial)
	} else {
		p.limiter.SetLimit(initial)
	}
	p.mu.Unlock()
	p.signalWake()
}

// SetTimeout replaces the network budgets for handshakes and latency probes.
func (p *ProxyPool) SetTimeout(c TimeoutConfig) {
	p.mu.Lock()
	p.timeouts = c.Resolve()
	p.mu.Unlock()
}

// RequestIngest schedules an asynchronous source ingest when the pool is running.
func (p *ProxyPool) RequestIngest() {
	select {
	case p.sched.ingestReq <- struct{}{}:
	default:
	}
}

// Start initializes persisted scheduler state and starts background ingest and dispatch.
// The method is non-blocking and is idempotent.
func (p *ProxyPool) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	p.sched.mu.Lock()
	if p.sched.start {
		p.sched.mu.Unlock()
		p.mu.Unlock()
		return
	}
	p.sched.start = true
	p.mode.Store(modeForegroundValue)
	p.starved.Store(false)
	p.sched.ctx, p.sched.cancel = context.WithCancel(ctx)
	p.sched.mu.Unlock()
	cache := p.cache
	cfg := p.config.Resolve()
	p.mu.Unlock()

	now := time.Now().UTC()
	dropUnchecked(cache, now)
	states := cache.All()
	p.index.replace(states)
	p.sched.rebuildHeaps(states, cfg, now)

	p.sched.wg.Add(8)
	go p.ingestLoop()
	go p.foregroundLaneLoop()
	go p.backgroundLaneLoop()
	go p.livenessLaneLoop()
	go p.revivalLaneLoop()
	go p.netProbeLoop()
	go p.modeLoop()
	go p.statsLoop()
	p.RequestIngest()
	p.signalWake()
}

// Stop cancels all background scheduler goroutines and waits for them to exit.
func (p *ProxyPool) Stop() {
	p.mu.RLock()
	s := p.sched
	p.mu.RUnlock()
	if s == nil {
		return
	}
	s.mu.Lock()
	if !s.start {
		s.mu.Unlock()
		return
	}
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()

	s.mu.Lock()
	s.start = false
	s.mu.Unlock()
}

// Refresh performs one blocking scheduler drain without starting background goroutines.
// It ingests all sources, then checks candidates and due cached proxies using the same
// queues, limiter, and checkOne implementation used by Start. It returns when candQ is
// empty and no live or banned items are due at the current time.
func (p *ProxyPool) Refresh() {
	// Refresh is a blocking one-shot drain for CLI and tests. It uses the same
	// queues and checkOne implementation as Start without starting background lanes.
	now := time.Now().UTC()
	p.ingestOnce(now)
	p.seedDueStates(now)

	ctx := context.Background()
	var wg sync.WaitGroup
	for {
		task, ok := p.nextRefreshTask(time.Now().UTC())
		if !ok {
			break
		}
		if err := p.limiter.Acquire(ctx); err != nil {
			p.requeueTask(task, time.Now().UTC().Add(30*time.Second))
			break
		}
		wg.Add(1)
		go func(task scheduledTask) {
			defer wg.Done()
			defer p.limiter.Release()
			p.runCheckHeld(ctx, task)
		}(task)
	}
	wg.Wait()
}

func (p *ProxyPool) nextTask(now time.Time) (scheduledTask, bool) {
	return p.nextRefreshTask(now)
}

func (p *ProxyPool) nextRefreshTask(now time.Time) (scheduledTask, bool) {
	cache := p.cacheSnapshot()
	cfg := p.configSnapshot()
	p.sched.mu.Lock()
	defer p.sched.mu.Unlock()
	for len(p.sched.forcedQ) > 0 {
		url := p.sched.forcedQ[0]
		p.sched.forcedQ = p.sched.forcedQ[1:]
		delete(p.sched.forced, url)
		if _, ok := p.sched.inflight[url]; ok {
			continue
		}
		state, ok := cache.Get(url)
		if !ok || !state.IsDead {
			continue
		}
		p.sched.inflight[url] = struct{}{}
		p.sched.laneInflight[string(laneRevival)]++
		return scheduledTask{url: url, lane: string(laneRevival), forced: true}, true
	}
	for p.sched.liveHeap.Len() > 0 {
		item := p.sched.liveHeap[0]
		if item.DueAt.After(now) {
			break
		}
		item = p.sched.liveHeap.PopDue(now)
		state, ok := cache.Get(item.URL)
		if !ok || state.IsDead {
			continue
		}
		expected := nextLiveDue(state, cfg)
		if !state.Suspect && !sameDue(item.DueAt, expected) {
			continue
		}
		if _, ok := p.sched.inflight[item.URL]; ok {
			continue
		}
		p.sched.inflight[item.URL] = struct{}{}
		p.sched.laneInflight[string(laneLiveness)]++
		return scheduledTask{url: item.URL, lane: string(laneLiveness)}, true
	}
	for p.sched.candHead < len(p.sched.candQ) {
		item := p.sched.candQ[p.sched.candHead]
		p.sched.candHead++
		delete(p.sched.queued, item.URL)
		if _, ok := p.sched.inflight[item.URL]; ok {
			continue
		}
		p.sched.inflight[item.URL] = struct{}{}
		p.sched.laneInflight[string(laneForeground)]++
		if p.sched.candHead > 1024 && p.sched.candHead*2 >= len(p.sched.candQ) {
			p.sched.candQ = append([]queuedCandidate(nil), p.sched.candQ[p.sched.candHead:]...)
			p.sched.candHead = 0
		}
		return scheduledTask{url: item.URL, source: item.Source, lane: string(laneForeground)}, true
	}
	if p.sched.candHead == len(p.sched.candQ) {
		p.sched.candQ = p.sched.candQ[:0]
		p.sched.candHead = 0
	}
	for p.sched.reviveHeap.Len() > 0 {
		item := p.sched.reviveHeap[0]
		if item.DueAt.After(now) {
			break
		}
		item = p.sched.reviveHeap.PopDue(now)
		state, ok := cache.Get(item.URL)
		if !ok || !state.IsDead || !sameDue(item.DueAt, state.ReviveAt) {
			continue
		}
		if _, ok := p.sched.inflight[item.URL]; ok {
			continue
		}
		p.sched.inflight[item.URL] = struct{}{}
		p.sched.laneInflight[string(laneRevival)]++
		return scheduledTask{url: item.URL, lane: string(laneRevival)}, true
	}
	return scheduledTask{}, false
}

type scheduledTask struct {
	url    string
	source string
	lane   string
	forced bool
}

func (p *ProxyPool) seedDueStates(now time.Time) {
	p.mu.RLock()
	cache := p.cache
	cfg := p.config.Resolve()
	p.mu.RUnlock()
	if cache == nil {
		return
	}
	for _, state := range cache.All() {
		p.sched.mu.Lock()
		_, inflight := p.sched.inflight[state.URL]
		if !inflight {
			if state.IsDead {
				if !state.ReviveAt.IsZero() && !state.ReviveAt.After(now) {
					p.sched.scheduleReviveLocked(state)
				}
			} else {
				due := nextLiveDue(state, cfg)
				if state.Suspect || (!due.IsZero() && !due.After(now)) {
					p.sched.scheduleLiveLocked(state, cfg)
				}
			}
		}
		p.sched.mu.Unlock()
	}
}

func (p *ProxyPool) unmarkInflight(url, lane string) {
	p.sched.mu.Lock()
	delete(p.sched.inflight, url)
	if lane != "" && p.sched.laneInflight[lane] > 0 {
		p.sched.laneInflight[lane]--
	}
	p.sched.mu.Unlock()
	p.signalWake()
}

func (p *ProxyPool) finishUnstarted(url string, completed bool) {
	if completed {
		return
	}
	p.sched.mu.Lock()
	delete(p.sched.inflight, url)
	p.sched.mu.Unlock()
	p.signalWake()
}

func (p *ProxyPool) requeueTask(task scheduledTask, due time.Time) {
	p.sched.mu.Lock()
	delete(p.sched.inflight, task.url)
	if task.lane != "" {
		if p.sched.laneInflight[task.lane] > 0 {
			p.sched.laneInflight[task.lane]--
		}
	}
	if task.forced {
		if _, ok := p.sched.forced[task.url]; !ok {
			p.sched.forced[task.url] = struct{}{}
			p.sched.forcedQ = append(p.sched.forcedQ, task.url)
		}
	} else if task.source != "" {
		if _, ok := p.sched.queued[task.url]; !ok {
			p.sched.queued[task.url] = struct{}{}
			p.sched.candQ = append(p.sched.candQ, queuedCandidate{URL: task.url, Source: task.source})
		}
	} else if task.lane == string(laneLiveness) {
		heapPush(&p.sched.liveHeap, task.url, due)
	} else if task.lane == string(laneRevival) {
		heapPush(&p.sched.reviveHeap, task.url, due)
	}
	p.sched.mu.Unlock()
	p.signalWake()
}

func (p *ProxyPool) runCheckHeld(ctx context.Context, task scheduledTask) {
	state, ok := p.loadTaskState(task)
	if !ok {
		p.unmarkInflight(task.url, task.lane)
		return
	}
	updated, report, ok := p.performCheck(ctx, state, task.lane, task.forced)
	if !ok {
		p.requeueTask(task, time.Now().UTC().Add(30*time.Second))
		return
	}

	p.mu.RLock()
	cache := p.cache
	reporter := p.reporter
	cfg := p.config.Resolve()
	p.mu.RUnlock()
	if current, exists := cache.Get(updated.URL); exists {
		updated = mergeManualMark(current, updated)
	}
	cache.Set(updated)
	p.finishState(updated, cfg, task.lane)
	reporter.ReportProxy(reportForState(report, updated))
}

func (p *ProxyPool) checkOne(ctx context.Context, state ProxyState, lane string) (ProxyState, ProxyReport, error) {
	return p.checkOneForced(ctx, state, lane, false)
}

func (p *ProxyPool) checkOneForced(ctx context.Context, state ProxyState, lane string, forced bool) (ProxyState, ProxyReport, error) {
	if err := p.limiter.Acquire(ctx); err != nil {
		return state, ProxyReport{}, err
	}
	defer p.limiter.Release()
	updated, report, ok := p.performCheck(ctx, state, lane, forced)
	if !ok {
		return state, report, errors.New("proxy check failed")
	}
	return updated, report, nil
}

func (p *ProxyPool) performCheck(ctx context.Context, state ProxyState, lane string, forced bool) (ProxyState, ProxyReport, bool) {
	p.mu.RLock()
	timeouts := p.timeouts.Resolve()
	p.mu.RUnlock()

	wasDead := state.IsDead
	now := time.Now().UTC()
	state.FailReason = FailNone

	hctx, cancel := context.WithTimeout(ctx, timeouts.Handshake)
	handshakeErr := verifyHandshake(hctx, state.URL, timeouts.Handshake)
	cancel()
	if handshakeErr != nil {
		state.FailReason = classifyError(wrapCheckError("handshake", handshakeErr))
		if !p.applyFailureResult(&state, now, state.FailReason, p.NetHealth(), forced, lane) {
			return state, ProxyReport{}, false
		}
		return state, ProxyReport{
			Timestamp:  now,
			URL:        state.URL,
			Source:     state.Source,
			Location:   state.Location,
			FailReason: state.FailReason,
			Lane:       lane,
			IsDead:     state.IsDead,
			Score:      state.Score,
			Penalty:    state.Penalty,
			ReviveAt:   state.ReviveAt,
			Latency:    0,
			Died:       !wasDead,
			Revived:    false,
		}, true
	}

	needLocation := state.Location == ""
	pctx, cancel := context.WithTimeout(ctx, timeouts.Probe)
	latency, location, probeErr := probeEndpoint(pctx, state.URL, needLocation, timeouts.Probe)
	cancel()
	if probeErr != nil {
		state.FailReason = classifyError(wrapCheckError("probe", probeErr))
		if !p.applyFailureResult(&state, now, state.FailReason, p.NetHealth(), forced, lane) {
			return state, ProxyReport{}, false
		}
		return state, ProxyReport{
			Timestamp:  now,
			URL:        state.URL,
			Source:     state.Source,
			Location:   state.Location,
			FailReason: state.FailReason,
			Lane:       lane,
			IsDead:     state.IsDead,
			Score:      state.Score,
			Penalty:    state.Penalty,
			ReviveAt:   state.ReviveAt,
			Latency:    0,
			Died:       !wasDead,
			Revived:    wasDead,
		}, true
	}

	applySuccess(&state, now, latency, location)
	return state, ProxyReport{
		Timestamp:  now,
		URL:        state.URL,
		Source:     state.Source,
		Location:   state.Location,
		FailReason: state.FailReason,
		Lane:       lane,
		IsDead:     state.IsDead,
		Score:      state.Score,
		Penalty:    state.Penalty,
		ReviveAt:   state.ReviveAt,
		Latency:    state.Latency,
		Died:       false,
		Revived:    wasDead,
	}, true
}

func (p *ProxyPool) applyFailureResult(state *ProxyState, now time.Time, reason FailReason, net NetSnapshot, forced bool, lane string) bool {
	if forced {
		applyFailureV2Config(state, now, reason, net, true, p.configSnapshot())
		return true
	}
	if lane == string(laneLiveness) && !state.IsDead && !state.Suspect && !state.LastCheckedAt.IsZero() {
		state.FailReason = reason
		state.LastCheckedAt = now
		state.IsDead = false
		state.Suspect = true
		state.ReviveAt = now.Add(10 * time.Second)
		return true
	}
	applyFailureV2Config(state, now, reason, net, false, p.configSnapshot())
	return true
}

func (p *ProxyPool) configSnapshot() Config {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.config.Resolve()
}

func reportForState(report ProxyReport, state ProxyState) ProxyReport {
	report.Source = state.Source
	report.Location = state.Location
	report.FailReason = state.FailReason
	report.IsDead = state.IsDead
	report.Score = state.Score
	report.Penalty = state.Penalty
	report.ReviveAt = state.ReviveAt
	report.Latency = state.Latency
	return report
}

func (p *ProxyPool) loadTaskState(task scheduledTask) (ProxyState, bool) {
	p.mu.RLock()
	cache := p.cache
	p.mu.RUnlock()
	if state, ok := cache.Get(task.url); ok {
		return state, true
	}
	if task.source == "" {
		return ProxyState{}, false
	}
	ep, err := normalizeProxyURL(task.url)
	if err != nil {
		return ProxyState{}, false
	}
	return ProxyState{URL: ep.Canonical, IP: ep.Host, Port: ep.Port, Source: task.source, LastSeenInSource: time.Now().UTC()}, true
}

func (p *ProxyPool) finishState(state ProxyState, cfg Config, lane string) {
	p.sched.mu.Lock()
	delete(p.sched.inflight, state.URL)
	if p.sched.laneInflight[lane] > 0 {
		p.sched.laneInflight[lane]--
	}
	if state.IsDead {
		p.sched.scheduleReviveLocked(state)
	} else {
		p.sched.scheduleLiveLocked(state, cfg)
	}
	p.sched.mu.Unlock()
	p.index.upsert(state)
	p.signalWake()
}

func (s *scheduler) scheduleLiveLocked(state ProxyState, cfg Config) {
	due := nextLiveDue(state, cfg)
	if !due.IsZero() {
		heapPush(&s.liveHeap, state.URL, due)
	}
}

func (s *scheduler) scheduleReviveLocked(state ProxyState) {
	if !state.ReviveAt.IsZero() {
		heapPush(&s.reviveHeap, state.URL, state.ReviveAt)
	}
}

func (s *scheduler) rebuildHeaps(states []ProxyState, cfg Config, now time.Time) {
	s.mu.Lock()
	s.liveHeap = nil
	s.reviveHeap = nil
	s.candQ = nil
	s.candHead = 0
	s.queued = make(map[string]struct{})
	s.forced = make(map[string]struct{})
	s.forcedQ = nil
	s.inflight = make(map[string]struct{})
	s.laneInflight = make(map[string]int)
	for _, state := range states {
		if state.IsDead {
			s.scheduleReviveLocked(state)
		} else {
			due := nextLiveDue(state, cfg)
			if state.Suspect {
				due = now
			}
			heapPush(&s.liveHeap, state.URL, due)
		}
	}
	s.mu.Unlock()
}

func nextLiveDue(state ProxyState, cfg Config) time.Time {
	if state.Suspect {
		if !state.ReviveAt.IsZero() {
			return state.ReviveAt
		}
		return state.LastCheckedAt
	}
	if state.LastCheckedAt.IsZero() {
		return time.Time{}
	}
	return state.LastCheckedAt.Add(cfg.LivenessInterval)
}

func sameDue(a, b time.Time) bool {
	return !a.IsZero() && !b.IsZero() && a.Equal(b)
}

func heapPush(h *scheduleHeap, url string, due time.Time) {
	if due.IsZero() {
		return
	}
	item := &scheduleItem{URL: url, DueAt: due}
	// Push without importing heap in this file via the local helper method below.
	heapPushItem(h, item)
}

func dropUnchecked(cache CacheSource, now time.Time) {
	if cache == nil {
		return
	}
	for _, state := range cache.All() {
		if state.LastCheckedAt.IsZero() {
			cache.Delete(state.URL)
			continue
		}
		if state.LastSeenInSource.IsZero() {
			state.LastSeenInSource = now
			cache.Set(state)
		}
	}
}

func (p *ProxyPool) ingestOnce(now time.Time) {
	p.mu.RLock()
	cache := p.cache
	sources := append([]ProxySource(nil), p.sources...)
	reporter := p.reporter
	cfg := p.config.Resolve()
	p.mu.RUnlock()
	if cache == nil {
		return
	}

	newCount, knownCount, total := 0, 0, 0
	for _, src := range sources {
		var items []TaggedURL
		if tagged, ok := src.(TaggedSource); ok {
			items = tagged.FetchTagged()
		} else {
			for _, raw := range src.FetchList() {
				items = append(items, TaggedURL{URL: raw, Source: src.Name()})
			}
		}
		for _, item := range items {
			ep, err := normalizeProxyURL(item.URL)
			if err != nil {
				continue
			}
			total++
			sourceName := item.Source
			if sourceName == "" {
				sourceName = src.Name()
			}
			state, exists := cache.Get(ep.Canonical)
			if exists {
				knownCount++
				changed := false
				if state.Source == "" {
					state.Source = sourceName
					changed = true
				}
				if now.Sub(state.LastSeenInSource) > time.Hour || state.LastSeenInSource.IsZero() {
					state.LastSeenInSource = now
					changed = true
				}
				if changed {
					cache.Set(state)
				}
				continue
			}

			p.sched.mu.Lock()
			_, alreadyQueued := p.sched.queued[ep.Canonical]
			_, alreadyInflight := p.sched.inflight[ep.Canonical]
			if !alreadyQueued && !alreadyInflight {
				p.sched.candQ = append(p.sched.candQ, queuedCandidate{URL: ep.Canonical, Source: sourceName})
				p.sched.queued[ep.Canonical] = struct{}{}
				newCount++
			}
			p.sched.mu.Unlock()
		}
	}

	// GC is restricted to persisted banned proxies that stopped appearing in sources.
	for _, state := range cache.All() {
		if state.IsDead && !state.LastSeenInSource.IsZero() && now.Sub(state.LastSeenInSource) > cfg.SourceGCAge {
			cache.Delete(state.URL)
			p.index.delete(state.URL)
		}
	}

	p.mu.Lock()
	p.config = cfg
	p.mu.Unlock()
	p.sched.mu.Lock()
	p.sched.lastIngestAt = now
	p.sched.mu.Unlock()
	p.refreshStarvedState()
	reporter.ReportEvent(PoolEvent{
		Time: now,
		Kind: "ingest_done",
		Fields: map[string]string{
			"new":   strconv.Itoa(newCount),
			"known": strconv.Itoa(knownCount),
			"total": strconv.Itoa(total),
		},
	})
	p.signalWake()
}

func (p *ProxyPool) ingestLoop() {
	defer p.sched.wg.Done()
	for {
		p.mu.RLock()
		interval := p.config.Resolve().IngestInterval
		ctx := p.sched.ctx
		p.mu.RUnlock()
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
			p.ingestOnce(time.Now().UTC())
		case <-p.sched.ingestReq:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			p.ingestOnce(time.Now().UTC())
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (p *ProxyPool) signalWake() {
	if p.sched == nil || p.sched.wake == nil {
		return
	}
	select {
	case p.sched.wake <- struct{}{}:
	default:
	}
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// Suspect marks a known alive proxy for an immediate liveness recheck without
// changing its reputation. The proxy remains out of ListProxies until a check succeeds.
func (p *ProxyPool) Suspect(url string, reason FailReason) bool {
	if url == "" {
		return false
	}
	p.mu.RLock()
	cache := p.cache
	p.mu.RUnlock()
	if cache == nil {
		return false
	}
	state, ok := cache.Get(url)
	if !ok || state.IsDead || state.Suspect {
		return false
	}
	now := time.Now().UTC()
	state.Suspect = true
	state.IsDead = false
	state.FailReason = reason
	state.ReviveAt = now
	state.LastCheckedAt = now
	cache.Set(state)
	p.index.upsert(state)
	p.sched.mu.Lock()
	delete(p.sched.inflight, url)
	heapPush(&p.sched.liveHeap, url, now)
	p.sched.mu.Unlock()
	p.signalWake()
	return true
}

// RecheckBanned queues banned proxies for the revival lane without changing their ban schedule.
func (p *ProxyPool) RecheckBanned(f BanFilter) int {
	states := p.cacheAll()
	queued := 0
	p.sched.mu.Lock()
	for _, state := range states {
		if !state.IsDead {
			continue
		}
		if f.Reason != FailNone && state.FailReason != f.Reason {
			continue
		}
		if f.Source != "" && state.Source != f.Source {
			continue
		}
		if _, ok := p.sched.inflight[state.URL]; ok {
			continue
		}
		if _, ok := p.sched.forced[state.URL]; ok {
			continue
		}
		p.sched.forced[state.URL] = struct{}{}
		p.sched.forcedQ = append(p.sched.forcedQ, state.URL)
		queued++
	}
	p.sched.mu.Unlock()
	if queued > 0 {
		p.signalWake()
	}
	return queued
}

// Manual exclusion schedule for MarkDead.
const (
	markDeadBaseBan      = 15 * time.Minute
	markDeadMaxBan       = 4 * time.Hour
	markDeadForgiveAfter = 24 * time.Hour
)

const (
	markCountKey  = "manual_dead_count"
	markLastAtKey = "manual_dead_last_at"
	markReasonKey = "manual_dead_reason"
)

// MarkDead excludes the proxy with the given canonical URL until an escalating ban expires.
func (p *ProxyPool) MarkDead(url, reason string) bool {
	if url == "" {
		return false
	}
	p.mu.Lock()
	if p.cache == nil {
		p.mu.Unlock()
		return false
	}
	state, ok := p.cache.Get(url)
	if !ok {
		p.mu.Unlock()
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
	state.Suspect = false
	if revive := now.Add(ban); revive.After(state.ReviveAt) {
		state.ReviveAt = revive
	}
	state.LastCheckedAt = now
	cache := p.cache
	reporter := p.reporter
	cfg := p.config.Resolve()
	p.mu.Unlock()

	cache.Set(state)
	p.finishState(state, cfg, "manual")
	reporter.ReportProxy(ProxyReport{
		Timestamp:  now,
		URL:        state.URL,
		Source:     state.Source,
		Location:   state.Location,
		FailReason: state.FailReason,
		Lane:       "manual",
		IsDead:     state.IsDead,
		Score:      state.Score,
		Penalty:    state.Penalty,
		ReviveAt:   state.ReviveAt,
		Latency:    state.Latency,
	})
	return true
}

func mergeManualMark(current, updated ProxyState) ProxyState {
	if markCount(current.Metadata) <= markCount(updated.Metadata) {
		return updated
	}
	updated.IsDead = true
	updated.Suspect = false
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

// UpdateMetadata runs update against the custom metadata map for the given proxy URL.
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

// GetMetadata returns a detached copy of custom metadata for the given proxy URL.
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

// ListProxies reads only the in-memory alive index.
func (p *ProxyPool) ListProxies(filter ProxyFilter) []ProxyInfo {
	return p.index.list(filter)
}
