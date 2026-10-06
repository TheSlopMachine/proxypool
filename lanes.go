package proxypool

import (
	"context"
	"sort"
	"time"
)

type lane string

const (
	laneForeground lane = "foreground"
	laneBackground lane = "background"
	laneLiveness   lane = "liveness"
	laneRevival    lane = "revival"
)

func laneCaps(mode Mode, starved bool, limit int) map[lane]int {
	if limit < 1 {
		limit = 1
	}
	caps := map[lane]int{
		laneForeground: 0,
		laneBackground: 0,
		laneLiveness:   0,
		laneRevival:    0,
	}
	if starved && mode == ModeForeground {
		caps[laneLiveness] = laneCap(0.15, limit, true)
		caps[laneRevival] = laneCap(0.85, limit, true)
		return caps
	}
	switch mode {
	case ModeBackground:
		caps[laneBackground] = laneCap(0.20, limit, true)
		caps[laneLiveness] = laneCap(0.50, limit, true)
		caps[laneRevival] = laneCap(0.30, limit, true)
	default:
		caps[laneForeground] = laneCap(0.80, limit, true)
		caps[laneLiveness] = laneCap(0.15, limit, true)
		caps[laneRevival] = laneCap(0.05, limit, true)
	}
	return caps
}

func laneCap(fraction float64, limit int, minimumOne bool) int {
	cap := int(float64(limit) * fraction)
	if float64(cap) < float64(limit)*fraction {
		cap++
	}
	if minimumOne && cap < 1 {
		cap = 1
	}
	if cap > limit {
		cap = limit
	}
	return cap
}

func (p *ProxyPool) foregroundLaneLoop() {
	defer p.sched.wg.Done()
	p.laneLoop(laneForeground)
}

func (p *ProxyPool) backgroundLaneLoop() {
	defer p.sched.wg.Done()
	p.laneLoop(laneBackground)
}

func (p *ProxyPool) livenessLaneLoop() {
	defer p.sched.wg.Done()
	p.laneLoop(laneLiveness)
}

func (p *ProxyPool) revivalLaneLoop() {
	defer p.sched.wg.Done()
	p.laneLoop(laneRevival)
}

func (p *ProxyPool) laneLoop(name lane) {
	var nextBackgroundStart time.Time
	for {
		if p.contextDone() {
			return
		}
		mode := p.Mode()
		if (name == laneForeground && mode != ModeForeground) || (name == laneBackground && mode != ModeBackground) {
			if !p.waitForWake(250 * time.Millisecond) {
				return
			}
			continue
		}
		if p.NetHealth().State == NetDown {
			if !p.waitNetUsableForLane() {
				return
			}
			continue
		}

		if name == laneBackground {
			limit := p.limiter.Limit()
			rate := max(1, limit*20/100/5)
			interval := time.Second / time.Duration(rate)
			if !nextBackgroundStart.IsZero() {
				remaining := time.Until(nextBackgroundStart)
				if remaining > 0 {
					if !p.waitForWake(remaining) {
						return
					}
					continue
				}
			}
			nextBackgroundStart = time.Now().Add(interval)
		}

		task, ok := p.nextLaneTask(name, time.Now().UTC())
		if !ok {
			if !p.waitForWake(250 * time.Millisecond) {
				return
			}
			continue
		}
		if !p.waitNetUsableForLane() {
			p.requeueTask(task, time.Now().UTC().Add(30*time.Second))
			return
		}
		if err := p.limiter.Acquire(p.sched.ctx); err != nil {
			p.requeueTask(task, time.Now().UTC().Add(30*time.Second))
			return
		}
		if p.NetHealth().State == NetDown {
			p.limiter.Release()
			p.requeueTask(task, time.Now().UTC().Add(30*time.Second))
			continue
		}
		p.sched.wg.Add(1)
		go func() {
			defer p.sched.wg.Done()
			defer p.limiter.Release()
			p.runCheckHeld(p.sched.ctx, task)
		}()
	}
}

func (p *ProxyPool) contextDone() bool {
	p.sched.mu.Lock()
	ctx := p.sched.ctx
	p.sched.mu.Unlock()
	if ctx == nil {
		return false
	}
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (p *ProxyPool) waitForWake(d time.Duration) bool {
	p.sched.mu.Lock()
	ctx := p.sched.ctx
	wake := p.sched.wake
	p.sched.mu.Unlock()
	if ctx == nil {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-wake:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *ProxyPool) waitNetUsable(ctx context.Context) bool {
	for {
		if p.NetHealth().State != NetDown {
			return true
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		}
	}
}

func (p *ProxyPool) waitNetUsableForLane() bool {
	p.sched.mu.Lock()
	ctx := p.sched.ctx
	p.sched.mu.Unlock()
	if ctx == nil {
		return false
	}
	return p.waitNetUsable(ctx)
}

func (p *ProxyPool) nextLaneTask(name lane, now time.Time) (scheduledTask, bool) {
	mode := p.Mode()
	starved := p.Starved()
	caps := laneCaps(mode, starved, p.limiter.Limit())
	cache := p.cacheSnapshot()
	cfg := p.configSnapshot()
	states := cache.All()
	p.sched.mu.Lock()
	defer p.sched.mu.Unlock()
	if p.sched.laneInflight[string(name)] >= caps[name] && !p.canBorrowLocked(name, now, caps[name]) {
		return scheduledTask{}, false
	}

	switch name {
	case laneForeground, laneBackground:
		for p.sched.candHead < len(p.sched.candQ) {
			item := p.sched.candQ[p.sched.candHead]
			p.sched.candHead++
			delete(p.sched.queued, item.URL)
			if _, ok := p.sched.inflight[item.URL]; ok {
				continue
			}
			p.sched.inflight[item.URL] = struct{}{}
			p.sched.laneInflight[string(name)]++
			if p.sched.candHead > 1024 && p.sched.candHead*2 >= len(p.sched.candQ) {
				p.sched.candQ = append([]queuedCandidate(nil), p.sched.candQ[p.sched.candHead:]...)
				p.sched.candHead = 0
			}
			return scheduledTask{url: item.URL, source: item.Source, lane: string(name)}, true
		}
		if p.sched.candHead == len(p.sched.candQ) {
			p.sched.candQ = p.sched.candQ[:0]
			p.sched.candHead = 0
		}
	case laneLiveness:
		for p.sched.liveHeap.Len() > 0 {
			item := p.sched.liveHeap[0]
			if item.DueAt.After(now) {
				break
			}
			item = p.sched.liveHeap.PopDue(now)
			state, ok := cache.Get(item.URL)
			if !ok {
				p.index.delete(item.URL)
				continue
			}
			if state.IsDead {
				p.sched.scheduleReviveLocked(state)
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
			p.sched.laneInflight[string(name)]++
			return scheduledTask{url: item.URL, lane: string(name)}, true
		}
	case laneRevival:
		for len(p.sched.forcedQ) > 0 {
			url := p.sched.forcedQ[0]
			p.sched.forcedQ = p.sched.forcedQ[1:]
			delete(p.sched.forced, url)
			state, ok := cache.Get(url)
			if !ok || !state.IsDead {
				continue
			}
			if _, ok := p.sched.inflight[url]; ok {
				continue
			}
			p.sched.inflight[url] = struct{}{}
			p.sched.laneInflight[string(name)]++
			return scheduledTask{url: url, lane: string(name), forced: true}, true
		}
		if p.Starved() {
			return p.nextStarvedRevivalLocked(name, now, states)
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
			p.sched.laneInflight[string(name)]++
			return scheduledTask{url: item.URL, lane: string(name)}, true
		}
	}
	return scheduledTask{}, false
}

func (p *ProxyPool) canBorrowLocked(name lane, now time.Time, cap int) bool {
	if p.limiter.Inflight() >= p.limiter.Limit() || cap == 0 && p.laneQueuedLocked(name, now) == 0 {
		return false
	}
	for other, count := range p.sched.laneInflight {
		if lane(other) == name || count == 0 {
			continue
		}
		if p.laneQueuedLocked(lane(other), now) > 0 {
			return false
		}
	}
	for _, other := range []lane{laneForeground, laneBackground, laneLiveness, laneRevival} {
		if other == name {
			continue
		}
		if p.laneQueuedLocked(other, now) > 0 {
			return false
		}
	}
	return true
}

func (p *ProxyPool) laneQueuedLocked(name lane, now time.Time) int {
	if name == laneForeground || name == laneBackground {
		return len(p.sched.candQ) - p.sched.candHead
	}
	if name == laneLiveness {
		count := 0
		for _, item := range p.sched.liveHeap {
			if !item.DueAt.After(now) {
				count++
			}
		}
		return count
	}
	count := len(p.sched.forcedQ)
	for _, item := range p.sched.reviveHeap {
		if !item.DueAt.After(now) {
			count++
		}
	}
	return count
}

func (p *ProxyPool) nextStarvedRevivalLocked(name lane, now time.Time, states []ProxyState) (scheduledTask, bool) {
	if name != laneRevival {
		return scheduledTask{}, false
	}
	candidates := make([]ProxyState, 0)
	for _, state := range states {
		if state.IsDead {
			if _, ok := p.sched.inflight[state.URL]; !ok {
				candidates = append(candidates, state)
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		si, sj := candidates[i], candidates[j]
		softI, softJ := si.FailReason.Soft(), sj.FailReason.Soft()
		if softI != softJ {
			return softI
		}
		if si.ReviveAt.IsZero() != sj.ReviveAt.IsZero() {
			return !si.ReviveAt.IsZero()
		}
		return si.ReviveAt.Before(sj.ReviveAt)
	})
	if len(candidates) == 0 {
		return scheduledTask{}, false
	}
	state := candidates[0]
	p.sched.inflight[state.URL] = struct{}{}
	p.sched.laneInflight[string(laneRevival)]++
	return scheduledTask{url: state.URL, lane: string(laneRevival)}, true
}

func (p *ProxyPool) cacheSnapshot() CacheSource {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cache
}

func (p *ProxyPool) cacheGet(url string) (ProxyState, bool) {
	cache := p.cacheSnapshot()
	if cache == nil {
		return ProxyState{}, false
	}
	return cache.Get(url)
}

func (p *ProxyPool) cacheAll() []ProxyState {
	p.mu.RLock()
	cache := p.cache
	p.mu.RUnlock()
	if cache == nil {
		return nil
	}
	return cache.All()
}
