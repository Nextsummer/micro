package manager

import (
	"context"
	"fmt"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/queue"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
		log.Error("new message client stream err: ", err)
	}

	startServerIOThreads(2, messageClientStream)

	for IsRunning() {

		sendQueue := GetServerNetworkManagerInstance().sendQueues[2]
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
		log.Error(err)
	}

	for {
		time.Sleep(time.Second * 10)
	}
}
