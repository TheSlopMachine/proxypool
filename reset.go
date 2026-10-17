package proxypool

import "time"

// Reset drops every in-memory structure derived from the cache: the alive
// index, the candidate and revival queues and the force override. Call it
// after the cache was cleared externally. The pool must be stopped; a running
// scheduler would keep working on tasks it already holds.
func (p *ProxyPool) Reset() {
	p.mu.RLock()
	cfg := p.config.Resolve()
	p.mu.RUnlock()
	p.index.replace(nil)
	p.sched.rebuildHeaps(nil, cfg, time.Now().UTC())
	p.sched.mu.Lock()
	p.sched.forceForeground = false
	p.sched.mu.Unlock()
}
