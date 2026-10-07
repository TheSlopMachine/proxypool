package proxypool

import (
	"sort"
	"strings"
	"time"
)

const (
	needTTL = 500 * time.Millisecond
	// replenishBoostFactor scales background candidate intensity while a country target is below its goal.
	replenishBoostFactor = 4
	demandReviveInterval = 2 * time.Second
	demandReviveBatch    = 20
	// demandReviveCooldown keeps one banned proxy from being force-checked on every tick.
	demandReviveCooldown = 2 * time.Minute
)

// demandNeed is the scheduling pressure derived from registered demands.
type demandNeed struct {
	// Hot means a demand has waiters, or nothing matches yet inside its hunting window.
	Hot bool
	// Replenish means some country target is below its goal inside its replenishment window.
	Replenish bool
	// Countries is the union of countries of demands that need work.
	Countries map[string]struct{}
}

// demandNeed returns the memoized scheduling pressure. The result is at most
// needTTL old unless invalidateNeed was called.
func (p *ProxyPool) demandNeed(now time.Time) demandNeed {
	p.needMu.Lock()
	if !p.needAt.IsZero() && now.Sub(p.needAt) < needTTL {
		need := p.need
		p.needMu.Unlock()
		return need
	}
	p.needMu.Unlock()

	counts, total := p.index.countByLocation()
	need := p.demands.evaluate(now, counts, total)

	label := "idle"
	switch {
	case need.Hot:
		label = "hot"
	case need.Replenish:
		label = "replenish"
	}
	p.needMu.Lock()
	p.need = need
	p.needAt = now
	changed := p.needLabel != label
	p.needLabel = label
	p.needMu.Unlock()
	if changed {
		p.demands.emit("demand_priority", map[string]string{
			"state":     label,
			"countries": joinCountrySet(need.Countries),
		})
	}
	return need
}

// invalidateNeed forces the next demandNeed call to recompute.
func (p *ProxyPool) invalidateNeed() {
	p.needMu.Lock()
	p.needAt = time.Time{}
	p.needMu.Unlock()
}

// laneMode returns the mode that governs lane gating. A hot demand overrides
// the pool mode with foreground.
func (p *ProxyPool) laneMode(need demandNeed) Mode {
	if need.Hot {
		return ModeForeground
	}
	return p.Mode()
}

// replenishBoost returns the background intensity multiplier.
func (p *ProxyPool) replenishBoost(need demandNeed, mode Mode) int {
	if mode == ModeBackground && need.Replenish && !need.Hot {
		return replenishBoostFactor
	}
	return 1
}

// demandReviveLoop feeds banned proxies of demanded countries into the forced
// queue. They are known to have worked once, so rechecking them is cheaper than
// searching fresh candidates.
func (p *ProxyPool) demandReviveLoop() {
	defer p.sched.wg.Done()
	ticker := time.NewTicker(demandReviveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.queueDemandRevivals(time.Now().UTC())
		case <-p.sched.ctx.Done():
			return
		}
	}
}

// queueDemandRevivals queues up to demandReviveBatch banned proxies located in
// a demanded country, soft failures first. It returns the number queued.
func (p *ProxyPool) queueDemandRevivals(now time.Time) int {
	need := p.demandNeed(now)
	if len(need.Countries) == 0 {
		return 0
	}
	var picks []ProxyState
	for _, state := range p.cacheSnapshot().All() {
		if !state.IsDead || now.Sub(state.LastCheckedAt) < demandReviveCooldown {
			continue
		}
		if _, ok := need.Countries[strings.ToUpper(state.Location)]; !ok {
			continue
		}
		picks = append(picks, state)
	}
	sort.Slice(picks, func(i, j int) bool {
		si, sj := picks[i].FailReason.Soft(), picks[j].FailReason.Soft()
		if si != sj {
			return si
		}
		return picks[i].ReviveAt.Before(picks[j].ReviveAt)
	})
	queued := 0
	p.sched.mu.Lock()
	for _, state := range picks {
		if queued >= demandReviveBatch {
			break
		}
		if _, busy := p.sched.inflight[state.URL]; busy {
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

// normalizeCountryHint returns an upper-case two-letter hint or "".
func normalizeCountryHint(raw string) string {
	code := strings.ToUpper(strings.TrimSpace(raw))
	if len(code) != 2 || code[0] < 'A' || code[0] > 'Z' || code[1] < 'A' || code[1] > 'Z' {
		return ""
	}
	return code
}

func joinCountrySet(set map[string]struct{}) string {
	if len(set) == 0 {
		return "any"
	}
	countries := make([]string, 0, len(set))
	for country := range set {
		countries = append(countries, country)
	}
	sort.Strings(countries)
	return strings.Join(countries, ",")
}
