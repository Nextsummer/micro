package utils

import (
	"github.com/Nextsummer/micro/pkg/queue"
	cmap "github.com/orcaman/concurrent-map/v2"
	"log"
	"testing"
)

func TestBytesToJson(t *testing.T) {
	slots := cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode)
	data := "{\"1\":{},\"2\":{},\"3\":{}}"
	BytesToJson([]byte(data), &slots)

	log.Println(slots.Keys())

	slotsReplicas := queue.NewArray[string]()
	BytesToJson([]byte(data), &slotsReplicas)
	log.Println(slotsReplicas)
}
