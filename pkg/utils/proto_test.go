package utils

import (
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
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

func TestEncodeAndDecode(t *testing.T) {
	request := pkgrpc.RegisterRequest{
		Request:             &pkgrpc.Request{Id: "1", Data: ToJsonByte("hello go")},
		ServiceName:         "hello",
		ServiceInstancePort: 8080,
		ServiceInstanceIp:   "127.0.0.1",
	}
	encode := GrpcEncode(&request)
	log.Println(encode)

	var registerRequest pkgrpc.RegisterRequest
	GrpcDecode(encode, &registerRequest)
	log.Println(ToJson(registerRequest))
}
