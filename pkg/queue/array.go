package queue

import (
	"encoding/json"
	"sync"
)

type Array[T any] struct {
	t []T
	sync.RWMutex
}

func NewArray[T any]() *Array[T] {
	return &Array[T]{}
}

func (a *Array[T]) Put(t ...T) {
	a.Lock()
	defer a.Unlock()
	a.t = append(a.t, t...)
}

func (a *Array[T]) PutAll(t []T) {
	a.Lock()
	defer a.Unlock()
	a.t = append(a.t, t...)
}

func (a *Array[T]) Take() (T, bool) {
	a.Lock()
	defer a.Unlock()
	var temp T

	if len(a.t) > 0 {
		t := a.t[0]
		a.t = append(a.t[:0], a.t[1:]...)
		return t, true
	}
	return temp, false
}

func (a *Array[T]) Size() int {
	a.RWMutex.RLock()
	defer a.RWMutex.RUnlock()
	return len(a.t)
}

func (a *Array[T]) IsEmpty() bool {
	a.RWMutex.RLock()
	defer a.RWMutex.RUnlock()
	return len(a.t) == 0
}

func (a *Array[T]) ClearAndIter() []T {
	a.Lock()
	defer a.Unlock()
	t := a.t
	a.t = make([]T, 0)
	return t
}

func (a *Array[T]) Iter() []T {
	a.RWMutex.RLock()
	defer a.RWMutex.RUnlock()
	return a.t
}

func (a *Array[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.t)
}

func (a *Array[T]) UnmarshalJSON(b []byte) error {
	var tmp []T
	if b == nil {
		return nil
	}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	a.PutAll(tmp)
	return nil
}
