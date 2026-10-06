package proxypool

import (
	"container/heap"
	"time"
)

type queuedCandidate struct {
	URL    string
	Source string
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
