package manager

import (
	"context"
	"fmt"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/queue"
	cmap "github.com/orcaman/concurrent-map/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"log"
	"net"
	"testing"
	"time"
)

func TestBufferExchange(t *testing.T) {
	s := &server{}

	current := make(map[int32]*queue.Array[pkgrpc.MessageEntity])
	q1 := queue.Array[pkgrpc.MessageEntity]{}
	q1.Put(pkgrpc.MessageEntity{RequestId: "1"})
	q1.Put(pkgrpc.MessageEntity{RequestId: "2"})

	q2 := queue.Array[pkgrpc.MessageEntity]{}
	q2.Put(pkgrpc.MessageEntity{RequestId: "A"})
	q2.Put(pkgrpc.MessageEntity{RequestId: "B"})

	current[1] = &q1
	current[2] = &q2

	exchange := s.bufferExchange(current)

	log.Printf("src: %v len: %v ", current, len(current))
	log.Printf("dst: %v len: %v ", exchange, len(exchange))

	for k, v := range current {
		log.Printf("src k: %v v: %v len: %v ", k, v, v.Size())
	}
	log.Println("==========================")

	for k, v := range exchange {
		log.Printf("dst k: %v v: %v len: %v ", k, v, len(v))
	}
}

func TestGrpcClient(t *testing.T) {
	Running()
	conn, err := grpc.Dial("127.0.0.1:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Did not connect, err: " + err.Error())
	}
	messageClientStream, err := pkgrpc.NewMessageClient(conn).Send(context.TODO())
	if err != nil {
		log.Fatal("new message client stream err: ", err)
	}

	startServerIO(2, messageClientStream)

	for IsRunning() {
		sendQueue, _ := GetServerNetworkManagerInstance().sendQueues.Get(2)
		sendQueue.Put(pkgrpc.MessageEntity{RequestId: "1001", Type: pkgrpc.MessageEntity_TERMINATE_MESSAGE})
		time.Sleep(time.Second * 5)

	}
}

func TestGrpcServer(t *testing.T) {

	Running()

	s := &server{}
	// 地址
	addr := "127.0.0.1:8080"
	// 1.监听
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("监听异常:%s\n", err)
	}
	fmt.Printf("监听端口：%s\n", addr)
	// 2.实例化gRPC
	grpcServer := grpc.NewServer()
	// 3.在gRPC上注册微服务
	pkgrpc.RegisterMessageServer(grpcServer, s)
	// 4.启动服务端
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}

	for {
		time.Sleep(time.Second * 10)
	}
}

func TestBytesBuffer(t *testing.T) {

	node := pkgrpc.ControllerVote{VoterNodeId: 313213, VoteRound: 2}
	marshal, err := proto.Marshal(&node)
	if err != nil {
		log.Println(err)
	}

	log.Println(marshal)

	serverNode := &pkgrpc.ControllerVote{}
	err = proto.Unmarshal(marshal, serverNode)
	if err != nil {
		log.Println(err)
	}
	log.Println(serverNode)

}

func TestSyncMap(t *testing.T) {
	m := cmap.New[string]()
	m.Set("1", "1")

	currentMap := cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode)

	array := queue.NewArray[string]()
	array.Put("str")
	array.Put("str2")
	array.Put("str3")

	currentMap.Set(1, array)
	array.Put("str4")

	array2 := queue.NewArray[string]()
	array2.Put("str")
	array2.Put("str2")
	currentMap.Set(2, array2)

	log.Println(currentMap.Get(1))
	log.Println(currentMap.Get(2))

}

func TestSetControllerNodeId(t *testing.T) {
	GetControllerNodeInstance().SetControllerNodeId(3)

	log.Println("node: ", GetControllerNodeInstance().nodeId)
}
