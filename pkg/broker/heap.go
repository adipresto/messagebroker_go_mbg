package broker

import (
	"mbg/pkg/models"
)

// MessageHeap implements heap.Interface and holds Messages.
// It is a min-heap based on NextRetry timestamp.
type MessageHeap[T any] []models.Message[T]

func (h MessageHeap[T]) Len() int           { return len(h) }
func (h MessageHeap[T]) Less(i, j int) bool { return h[i].NextRetry < h[j].NextRetry }
func (h MessageHeap[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MessageHeap[T]) Push(x any) {
	*h = append(*h, x.(models.Message[T]))
}

func (h *MessageHeap[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h MessageHeap[T]) Peek() (models.Message[T], bool) {
	if len(h) == 0 {
		var zero models.Message[T]
		return zero, false
	}
	return h[0], true
}
