package proxypool

import (
	"container/heap"
	"testing"
	"time"
)

func TestScheduleHeapOrdersByDueAt(t *testing.T) {
	now := time.Now()
	var h scheduleHeap
	heap.Init(&h)
	heap.Push(&h, &scheduleItem{URL: "late", DueAt: now.Add(2 * time.Second)})
	heap.Push(&h, &scheduleItem{URL: "early", DueAt: now.Add(time.Second)})
	item := h.PopDue(now.Add(1500 * time.Millisecond))
	if item == nil || item.URL != "early" {
		t.Fatalf("got %+v, want early item", item)
	}
	if h.Len() != 1 || h[0].URL != "late" {
		t.Fatalf("heap order corrupted: %+v", h)
	}
}

func TestSchedulerDeduplicatesQueuedAndInflight(t *testing.T) {
	p := NewPool()
	p.sched.mu.Lock()
	p.sched.candQ = append(p.sched.candQ, queuedCandidate{URL: "http://1.2.3.4:8080", Source: "one"})
	p.sched.queued["http://1.2.3.4:8080"] = struct{}{}
	p.sched.candQ = append(p.sched.candQ, queuedCandidate{URL: "http://1.2.3.4:8080", Source: "two"})
	p.sched.queued["http://1.2.3.4:8080"] = struct{}{}
	p.sched.inflight["http://5.6.7.8:8080"] = struct{}{}
	p.sched.mu.Unlock()

	if task, ok := p.nextTask(time.Now()); !ok || task.url != "http://1.2.3.4:8080" {
		t.Fatalf("expected first queued URL, got %+v/%v", task, ok)
	}
	p.sched.mu.Lock()
	if _, ok := p.sched.inflight["http://1.2.3.4:8080"]; !ok {
		t.Fatal("selected URL must become inflight")
	}
	p.sched.mu.Unlock()

	p.finishUnstarted("http://1.2.3.4:8080", false)
	p.sched.mu.Lock()
	if _, ok := p.sched.inflight["http://1.2.3.4:8080"]; ok {
		t.Fatal("finished URL must leave inflight")
	}
	if _, ok := p.sched.inflight["http://5.6.7.8:8080"]; !ok {
		t.Fatal("unrelated inflight marker must remain")
	}
	p.sched.mu.Unlock()
}
