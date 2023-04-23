package queue

import (
	"encoding/json"
	"fmt"
	"github.com/Nextsummer/micro/pkg/utils"
	"math/rand"
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

func (a *Array[T]) Take() (t T, result bool) {
	a.Lock()
	defer a.Unlock()

	if len(a.t) > 0 {
		t := a.t[0]
		a.t = append(a.t[:0], a.t[1:]...)
		return t, true
	}
	return t, false
}

func (a *Array[T]) Remove(t T) {
	a.Lock()
	defer a.Unlock()

	if len(a.t) > 0 {
		for i := 0; i < len(a.t); i++ {
			if fmt.Sprintf("%v", a.t[i]) == fmt.Sprintf("%v", t) {
				a.t = append(a.t[:i], a.t[i+1:]...)
				break
			}
		}
	}
}

func (a *Array[T]) Contains(t T) bool {
	a.RWMutex.Lock()
	defer a.RWMutex.Unlock()

	if len(a.t) > 0 {
		for i := 0; i < len(a.t); i++ {
			if fmt.Sprintf("%v", a.t[i]) == fmt.Sprintf("%v", t) {
				return true
			}
		}
	}
	return false
}

func (a *Array[T]) RandomTake() (t T) {
	a.Lock()
	defer a.Unlock()

	if len(a.t) > 0 {
		randNum := rand.Intn(len(a.t))
		t := a.t[randNum]
		a.t = append(a.t[:randNum], a.t[randNum+1:]...)
		return t
	}
	return t
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

func (a *Array[T]) Delete(i int) {
	a.RWMutex.Lock()
	defer a.RWMutex.Unlock()
	a.t = append(a.t[:i], a.t[i+1:]...)
}

func (a *Array[T]) MarshalJSON() ([]byte, error) {
	a.RWMutex.RLock()
	defer a.RWMutex.RUnlock()
	return json.Marshal(a.t)
}

func (a *Array[T]) UnmarshalJSON(b []byte) error {
	a.RWMutex.Lock()
	defer a.RWMutex.Unlock()
	var tmp []T
	if b == nil {
		return nil
	}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	a.t = append(a.t, tmp...)
	return nil
}

func (a *Array[T]) String() string {
	a.RWMutex.RLock()
	defer a.RWMutex.RUnlock()
	return utils.ToJson(a.t)
}
