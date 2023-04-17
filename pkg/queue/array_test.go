package queue

import (
	"fmt"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
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
	array := Array[pkgrpc.MessageResponse]{}

	response1 := pkgrpc.MessageResponse{Success: true, Message: "hello1"}
	response2 := pkgrpc.MessageResponse{Success: true, Message: "hello2"}

	array.Put(response1)
	array.Put(response2)

	array.Remove(response2)

	fmt.Println(array.String())

}
