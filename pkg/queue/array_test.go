package queue

import (
	log "github.com/sirupsen/logrus"
	"testing"
)

func TestClearAndClone(t *testing.T) {
	array := Array[string]{}

	array.Put("string1")
	array.Put("string2")
	array.Put("string3")

	log.Printf("src array: %v, len: %v, cap: %v", array, len(array.t), cap(array.t))

	clone := array.ClearAndClone()

	log.Printf("clone array: %v, len: %v, cap: %v", clone, len(clone), cap(clone))
	log.Printf("clone after array: %v, len: %v, cap: %v", array, len(array.t), cap(array.t))

	array.Put("4")

	log.Printf("put after array: %v, len: %v, cap: %v", array, len(array.t), cap(array.t))

}
