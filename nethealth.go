package proxypool

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	NetGood     NetState = "good"
	NetDegraded NetState = "degraded"
	NetDown     NetState = "down"
)

type netSample struct {
	ok  bool
	rtt time.Duration
}

type netHealth struct {
	client   *http.Client
	probe    func(context.Context, string) (time.Duration, error)
	snapshot atomic.Pointer[NetSnapshot]
}

func newNetHealth() *netHealth {
	h := &netHealth{
		client: &http.Client{
			Transport: &http.Transport{
				Proxy:             nil,
				DisableKeepAlives: true,
			},
		},
	}
	h.probe = h.probeURL
	h.snapshot.Store(&NetSnapshot{State: NetGood})
	return h
}

func (h *netHealth) probeURL(ctx context.Context, rawURL string) (time.Duration, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return 0, errBadProbeStatus
	}
	return time.Since(start), nil
}

func summarizeNetSamples(samples []netSample, degradedRTT time.Duration) NetState {
	if len(samples) < 3 {
		return NetGood
	}
	allFailed := true
	anyFailed := false
	latencies := make([]time.Duration, 0, 3)
	for _, sample := range samples {
		if sample.ok {
			allFailed = false
			latencies = append(latencies, sample.rtt)
		} else {
			anyFailed = true
		}
	}
	if allFailed {
		return NetDown
	}
	if anyFailed {
		return NetDegraded
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		median := latencies[len(latencies)/2]
		if median > degradedRTT {
			return NetDegraded
		}
	}
	return NetGood
}

func medianSuccessfulRTT(samples []netSample) time.Duration {
	latencies := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		if sample.ok {
			latencies = append(latencies, sample.rtt)
		}
	}
	if len(latencies) == 0 {
		return 0
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencies[len(latencies)/2]
}

func (p *ProxyPool) NetHealth() NetSnapshot {
	p.mu.RLock()
	h := p.netHealth
	p.mu.RUnlock()
	if h == nil {
		return NetSnapshot{State: NetGood}
	}
	if snapshot := h.snapshot.Load(); snapshot != nil {
		return *snapshot
	}
	return NetSnapshot{State: NetGood}
}

func (p *ProxyPool) netProbeLoop() {
	defer p.sched.wg.Done()
	p.mu.RLock()
	cfg := p.config.Resolve()
	h := p.netHealth
	p.mu.RUnlock()
	if h == nil {
		return
	}

	ticker := time.NewTicker(cfg.NetProbeInterval)
	defer ticker.Stop()
	urls := []string{"http://cp.cloudflare.com/generate_204", "http://connectivitycheck.gstatic.com/generate_204"}
	samples := make([]netSample, 0, 3)
	urlIndex := 0
	goodStreak := 0
	lastLimitEvent := time.Time{}
	lastState := NetGood

	probeOnce := func() {
		ctx, cancel := context.WithTimeout(p.sched.ctx, 2*time.Second)
		rtt, err := h.probe(ctx, urls[urlIndex])
		cancel()
		urlIndex = (urlIndex + 1) % len(urls)
		sample := netSample{ok: err == nil, rtt: rtt}
		samples = append(samples, sample)
		if len(samples) > 3 {
			samples = samples[len(samples)-3:]
		}
		state := summarizeNetSamples(samples, cfg.NetDegradedRTT)
		now := time.Now().UTC()
		rttMedian := medianSuccessfulRTT(samples)
		h.snapshot.Store(&NetSnapshot{State: state, RTT: rttMedian, UpdatedAt: now})

		if state != lastState {
			p.mu.RLock()
			reporter := p.reporter
			p.mu.RUnlock()
			reporter.ReportEvent(PoolEvent{
				Time: now,
				Kind: "net_change",
				Fields: map[string]string{
					"from":   string(lastState),
					"to":     string(state),
					"rtt_ms": strconvFormatDuration(rttMedian),
				},
			})
			if lastState != NetDown && state == NetDown {
				reporter.ReportEvent(PoolEvent{Time: now, Kind: "breaker_open", Fields: map[string]string{"net": string(state)}})
			}
			if lastState == NetDown && state != NetDown {
				reporter.ReportEvent(PoolEvent{Time: now, Kind: "breaker_close", Fields: map[string]string{"net": string(state)}})
			}
			lastState = state
		}

		p.mu.RLock()
		currentLimit := p.limiter.Limit()
		before := currentLimit
		goodStreakLimit := goodStreak
		cfgNow := p.config.Resolve()
		p.mu.RUnlock()
		newLimit, newGoodStreak := nextLimit(currentLimit, state, goodStreakLimit, cfgNow)
		goodStreak = newGoodStreak
		if newLimit != before {
			p.limiter.SetLimit(newLimit)
			if lastLimitEvent.IsZero() || now.Sub(lastLimitEvent) >= 10*time.Second {
				p.mu.RLock()
				reporter := p.reporter
				p.mu.RUnlock()
				reporter.ReportEvent(PoolEvent{
					Time: now,
					Kind: "limit_change",
					Fields: map[string]string{
						"from": strconvInt(before),
						"to":   strconvInt(newLimit),
						"net":  string(state),
					},
				})
				lastLimitEvent = now
			}
		}
	}

	for {
		select {
		case <-p.sched.ctx.Done():
			return
		default:
		}
		probeOnce()
		select {
		case <-ticker.C:
		case <-p.sched.ctx.Done():
			return
		}
	}
}

func strconvFormatDuration(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}

func strconvInt(v int) string {
	return strconv.Itoa(v)
}
