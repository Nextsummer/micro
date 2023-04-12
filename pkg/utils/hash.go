package utils

import (
	"fmt"
	"hash/fnv"
)

var Int32HashCode = func(key int32) uint32 {
	hash := fnv.New32()
	hash.Write([]byte(fmt.Sprintf("%v", key)))
	return hash.Sum32() >> 24
}
