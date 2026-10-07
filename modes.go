package proxypool

import (
	"strconv"
	"time"
)

const (
	modeForegroundValue int32 = 1
	modeBackgroundValue int32 = 2
)

func (p *ProxyPool) ensureModeState() {
	if p.mode.Load() == 0 {
		p.mode.Store(modeForegroundValue)
	}
}

func (p *ProxyPool) Mode() Mode {
	p.ensureModeState()
	if p.mode.Load() == modeBackgroundValue {
		return ModeBackground
	}
	return ModeForeground
}

func (p *ProxyPool) Starved() bool {
	return p.starved.Load()
}

func (p *ProxyPool) modeLoop() {
	defer p.sched.wg.Done()
	p.ensureModeState()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.updateModeState()
		case <-p.sched.ctx.Done():
			return
		}
	}
}

func nextMode(current Mode, alive int, cfg Config) Mode {
	cfg = cfg.Resolve()
	if current == ModeBackground && alive < cfg.LowWater {
		return ModeForeground
	}
	if current == ModeForeground && alive >= cfg.HighWater {
		return ModeBackground
	}
	return current
}

func (p *ProxyPool) updateModeState() {
	p.mu.RLock()
	cfg := p.config.Resolve()
	reporter := p.reporter
	p.mu.RUnlock()
	alive := len(p.index.list(ProxyFilter{}))
	current := p.Mode()
	next := nextMode(current, alive, cfg)
	if next != current {
		p.storeMode(next)
		reporter.ReportEvent(PoolEvent{
			Time: time.Now().UTC(),
			Kind: "mode_change",
			Fields: map[string]string{
				"from":  string(current),
				"to":    string(next),
				"alive": strconv.Itoa(alive),
			},
		})
	}
	p.refreshStarvedState()
}

func (p *ProxyPool) storeMode(mode Mode) {
	if mode == ModeBackground {
		p.mode.Store(modeBackgroundValue)
	} else {
		p.mode.Store(modeForegroundValue)
	}
	p.signalWake()
}

func (p *ProxyPool) refreshStarvedState() {
	p.mu.RLock()
	cfg := p.config.Resolve()
	reporter := p.reporter
	p.mu.RUnlock()
	alive := len(p.index.list(ProxyFilter{}))
	p.sched.mu.Lock()
	candEmpty := len(p.sched.queued) == 0
	candInflight := p.sched.laneInflight[string(laneForeground)] + p.sched.laneInflight[string(laneBackground)]
	p.sched.mu.Unlock()
	starved := p.Mode() == ModeForeground && alive < cfg.LowWater && candEmpty && candInflight == 0
	old := p.starved.Swap(starved)
	if old != starved {
		reporter.ReportEvent(PoolEvent{
			Time:   time.Now().UTC(),
			Kind:   "starved_change",
			Fields: map[string]string{"starved": strconv.FormatBool(starved)},
		})
		p.signalWake()
	}
}
