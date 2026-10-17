package proxypool

// ForceForeground makes the foreground lane work through the candidate queue
// at full speed regardless of the pool mode. In background mode candidates are
// otherwise checked slowly (a fifth of the limit, rate-limited).
//
// The override ends by itself once the candidate queue is empty and no
// candidate check is in flight. Candidates added by a later ingest are not
// covered. It returns the number of candidates waiting at the call; zero means
// there is nothing to force and no override is set.
func (p *ProxyPool) ForceForeground() int {
	if p.sched == nil {
		return 0
	}
	p.sched.mu.Lock()
	queued := len(p.sched.queued)
	if queued == 0 {
		p.sched.mu.Unlock()
		return 0
	}
	p.sched.forceForeground = true
	p.sched.mu.Unlock()
	p.signalWake()
	return queued
}

// forcedForeground reports whether the ForceForeground override is active.
func (p *ProxyPool) forcedForeground() bool {
	p.sched.mu.Lock()
	defer p.sched.mu.Unlock()
	return p.forcedForegroundLocked()
}

// forcedForegroundLocked is forcedForeground for callers holding sched.mu. It
// clears the override once the candidate queue has drained.
func (p *ProxyPool) forcedForegroundLocked() bool {
	if !p.sched.forceForeground {
		return false
	}
	inflight := p.sched.laneInflight[string(laneForeground)] + p.sched.laneInflight[string(laneBackground)]
	if len(p.sched.queued) == 0 && inflight == 0 {
		p.sched.forceForeground = false
		return false
	}
	return true
}
