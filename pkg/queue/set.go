package queue

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/utils"
)

type Set[T any] struct {
	sets map[int32]T
}

func NewSet[T any]() *Set[T] {
	return &Set[T]{make(map[int32]T)}
}

func (s *Set[T]) Add(t T) {
	s.sets[utils.StringHashCode(fmt.Sprintf("%v", t))] = t
}

func (s *Set[T]) Iter() (t []T) {
	if len(s.sets) == 0 {
		return
	}
	for k := range s.sets {
		t = append(t, s.sets[k])
	}
	return
}

func (s *Set[T]) Clear() {
	s.sets = make(map[int32]T)
}
