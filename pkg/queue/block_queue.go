package queue

import (
	"math"
	"sync"
)

type Queue[T any] interface {
	Push(T)
	Pop() T
}

type BlockQueue[T any] struct {
	lock     sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	data     []any
	head     int
	tail     int
	count    int
	capacity int
}

func NewBlockQueue[T any]() *BlockQueue[T] {
	return NewBlockQueueWithCap[T](math.MaxInt32)
}

func NewBlockQueueWithCap[T any](capacity int) *BlockQueue[T] {
	bq := &BlockQueue[T]{
		data:     make([]any, capacity),
		capacity: capacity,
	}
	bq.notEmpty = sync.NewCond(&bq.lock)
	bq.notFull = sync.NewCond(&bq.lock)
	return bq
}

func (bq *BlockQueue[T]) Push(item T) {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	for bq.count == bq.capacity {
		bq.notFull.Wait()
	}
	bq.data[bq.tail] = item
	bq.tail = (bq.tail + 1) % bq.capacity
	bq.count++
	bq.notEmpty.Signal()
}

// Pop This is only supported when methods use coroutines
// todo To be solved:  cannot use item (variable of type any) as type T in return statement
func (bq *BlockQueue[T]) Pop() any {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	for bq.count == 0 {
		bq.notEmpty.Wait()
	}
	item := bq.data[bq.head]
	bq.head = (bq.head + 1) % bq.capacity
	bq.count--
	bq.notFull.Signal()
	return item
}
