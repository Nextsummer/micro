package core

import (
	"fmt"
	"github.com/Nextsummer/micro/client/config"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestServiceInstance_Register(t *testing.T) {
	log.InitLog("./client.log")
	serviceInstance := NewServiceInstance()
	serviceInstance.Init()
	serviceInstance.Register()
	for {
		time.Sleep(time.Second * 3)

	}
}

func TestNetworkIO(t *testing.T) {
	log.InitLog("/client.log")
	serviceInstance := NewServiceInstance()
	server := *config.NewServer("127.0.0.1", 5002)
	serviceInstance.serverConnection = serviceInstance.connectServer(server)

	serviceInstance.networkIO()

	response := serviceInstance.sendRequest(&pkgrpc.MessageEntity{RequestId: uuid.New().String()}, server)

	fmt.Println("response: ", response)
	for {
		time.Sleep(time.Second)
	}
}

func TestForIter(t *testing.T) {
	array := queue.NewArray[string]()
	array.Put("123")
	array.Put("1232")
	array.Put("1233")
	array.Put("1234")

	for _, a := range array.Iter() {
		fmt.Println(a)
	}
}
