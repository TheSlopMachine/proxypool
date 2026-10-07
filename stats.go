package proxypool

import (
	"sort"
	"time"
)

// Snapshot returns the current pool state without performing network I/O.
func (p *ProxyPool) Snapshot() PoolStats {
	now := time.Now().UTC()
	states := p.cacheAll()
	stats := PoolStats{
		Time:       now,
		Mode:       p.Mode(),
		Net:        p.NetHealth(),
		Limit:      p.limiter.Limit(),
		Inflight:   p.limiter.Inflight(),
		Lanes:      make([]LaneStats, 0, 4),
		Sources:    make([]SourceStats, 0),
		BanReasons: make(map[FailReason]int),
	}
	bySource := make(map[string]*SourceStats)
	for _, state := range states {
		ss := sourceStat(bySource, state.Source)
		switch {
		case state.IsDead:
			stats.Banned++
			ss.Banned++
			if state.FailReason != FailNone {
				stats.BanReasons[state.FailReason]++
				ss.BanReasons[state.FailReason]++
			}
		case state.Suspect:
			stats.Suspect++
			ss.Suspect++
		default:
			stats.Alive++
			ss.Alive++
		}
	}

	p.sched.mu.Lock()
	stats.Queued = len(p.sched.queued)
	queuedBySource := make(map[string]int)
	for n := p.sched.candHead; n < len(p.sched.candQ); n++ {
		// Entries consumed through a hint queue stay in candQ until the head passes them.
		if _, ok := p.sched.queued[p.sched.candQ[n].URL]; ok {
			queuedBySource[p.sched.candQ[n].Source]++
		}
	}
	for source, count := range queuedBySource {
		sourceStat(bySource, source).Queued = count
	}
	for _, name := range []lane{laneForeground, laneBackground, laneLiveness, laneRevival} {
		queued := p.laneQueuedLocked(name, now)
		if name == laneForeground && stats.Mode == ModeBackground {
			queued = 0
		}
		if name == laneBackground && stats.Mode == ModeForeground {
			queued = 0
		}
		stats.Lanes = append(stats.Lanes, LaneStats{
			Lane:     string(name),
			Inflight: p.sched.laneInflight[string(name)],
			Queued:   queued,
		})
	}
	stats.LastIngestAt = p.sched.lastIngestAt
	p.sched.mu.Unlock()
	counts, total := p.index.countByLocation()
	stats.Demands = p.demands.snapshotWithCounts(now, counts, total)

	for _, ss := range bySource {
		ss.BanReasons = copyReasonMap(ss.BanReasons)
		stats.Sources = append(stats.Sources, *ss)
	}
	sort.Slice(stats.Sources, func(i, j int) bool { return stats.Sources[i].Source < stats.Sources[j].Source })
	return stats
}

func (p *ProxyPool) statsLoop() {
	defer p.sched.wg.Done()
	for {
		p.mu.RLock()
		interval := p.config.Resolve().StatsInterval
		ctx := p.sched.ctx
		reporter := p.reporter
		p.mu.RUnlock()
		if interval <= 0 {
			interval = 30 * time.Second
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
			reporter.ReportStats(p.Snapshot())
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

func sourceStat(m map[string]*SourceStats, source string) *SourceStats {
	ss := m[source]
	if ss == nil {
		ss = &SourceStats{Source: source, BanReasons: make(map[FailReason]int)}
		m[source] = ss
	}
	return ss
}

func copyReasonMap(in map[FailReason]int) map[FailReason]int {
	if len(in) == 0 {
		return make(map[FailReason]int)
	}
	out := make(map[FailReason]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
