package queue

import (
	"github.com/Nextsummer/micro/pkg/log"
	"testing"
)

func TestClearAndClone(t *testing.T) {
	array := Array[string]{}

	array.Put("string1")
	array.Put("string2")
	array.Put("string3")

	log.Info.Printf("src array: %v, len: %v, cap: %v", array, len(array.t), cap(array.t))

	clone := array.ClearAndIter()

	log.Info.Printf("clone array: %v, len: %v, cap: %v", clone, len(clone), cap(clone))
	log.Info.Printf("clone after array: %v, len: %v, cap: %v", array, len(array.t), cap(array.t))

	array.Put("4")

	log.Info.Printf("put after array: %v, len: %v, cap: %v", array, len(array.t), cap(array.t))

}

func TestArray_Remove(t *testing.T) {
	array := Array[string]{}

	array.Put("string1")
	array.Put("string2")
	array.Put("string3")

	array.Remove("string1")
}
