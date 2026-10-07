package proxypool

import (
	"container/heap"
	"sort"
	"time"
)

type queuedCandidate struct {
	URL     string
	Source  string
	Country string // upper-case source hint, may be empty
}

// takeQueuedLocked consumes the queued marker of url. It reports false when the
// URL was already consumed through another queue or is being checked right now.
func (s *scheduler) takeQueuedLocked(url string) bool {
	if _, ok := s.queued[url]; !ok {
		return false
	}
	delete(s.queued, url)
	_, busy := s.inflight[url]
	return !busy
}

// popCandidateLocked removes the next candidate. Candidates whose source hint
// is in want come first; the rest follow in arrival order.
func (s *scheduler) popCandidateLocked(want map[string]struct{}) (queuedCandidate, bool) {
	if len(want) > 0 {
		countries := make([]string, 0, len(want))
		for country := range want {
			countries = append(countries, country)
		}
		sort.Strings(countries)
		for _, country := range countries {
			list := s.hinted[country]
			for len(list) > 0 {
				item := list[0]
				list = list[1:]
				if s.takeQueuedLocked(item.URL) {
					s.hinted[country] = list
					return item, true
				}
			}
			delete(s.hinted, country)
		}
	}
	for s.candHead < len(s.candQ) {
		item := s.candQ[s.candHead]
		s.candHead++
		if !s.takeQueuedLocked(item.URL) {
			continue
		}
		if s.candHead > 1024 && s.candHead*2 >= len(s.candQ) {
			s.candQ = append([]queuedCandidate(nil), s.candQ[s.candHead:]...)
			s.candHead = 0
		}
		return item, true
	}
	if s.candHead == len(s.candQ) {
		s.candQ = s.candQ[:0]
		s.candHead = 0
	}
	return queuedCandidate{}, false
}

// pruneHintedLocked drops hint entries whose candidate is no longer queued.
func (s *scheduler) pruneHintedLocked() {
	for country, list := range s.hinted {
		kept := list[:0]
		for _, item := range list {
			if _, ok := s.queued[item.URL]; ok {
				kept = append(kept, item)
			}
		}
		if len(kept) == 0 {
			delete(s.hinted, country)
			continue
		}
		s.hinted[country] = kept
	}
}

type scheduleItem struct {
	URL   string
	DueAt time.Time
	index int
}

type scheduleHeap []*scheduleItem

func (h scheduleHeap) Len() int { return len(h) }
func (h scheduleHeap) Less(i, j int) bool {
	return h[i].DueAt.Before(h[j].DueAt)
}
func (h scheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *scheduleHeap) Push(x any) {
	item := x.(*scheduleItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *scheduleHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

func (h *scheduleHeap) PopDue(now time.Time) *scheduleItem {
	if h.Len() == 0 || (*h)[0].DueAt.After(now) {
		return nil
	}
	return heap.Pop(h).(*scheduleItem)
}

func heapPushItem(h *scheduleHeap, item *scheduleItem) {
	heap.Push(h, item)
}
