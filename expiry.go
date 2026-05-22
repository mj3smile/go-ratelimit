package ratelimit

import "time"

type mapType int

const (
	mapEntries mapType = iota
	mapSliding
	mapTokens
	mapGCRA
)

type heapKey struct {
	mt  mapType
	key string
}

type heapItem struct {
	key    string
	mt     mapType
	expiry time.Time
	index  int
}

type expiryHeap []*heapItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].expiry.Before(h[j].expiry) }
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *expiryHeap) Push(x any) {
	item := x.(*heapItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}