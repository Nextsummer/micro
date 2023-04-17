package core

import (
	"fmt"
	"github.com/Nextsummer/micro/client/config"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/google/uuid"
	"io"
	"net"
	"testing"
	"time"
)

func TestServiceInstance_Register(t *testing.T) {
	log.InitLog("./client.log")
	serviceInstance := NewServiceInstance()
	serviceInstance.Init()
	serviceInstance.Register()
	serviceInstance.Subscribe("ORDER-SERVICE")
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

func TestHeartbeat(t *testing.T) {
	log.InitLog("/temp.log")

	heartbeatRequest := NewMessageEntity(pkgrpc.MessageEntity_CLIENT_HEARTBEAT, utils.Encode(&pkgrpc.HeartbeatRequest{
		ServiceName:         "Order-Center",
		ServiceInstanceIp:   "localhost",
		ServiceInstancePort: 8080,
	}))

	//FetchServerNodeIdRequest := NewMessageEntity(pkgrpc.MessageEntity_CLIENT_FETCH_SERVER_NODE_ID, nil)

	conn, err := net.Dial("tcp", "127.0.0.1:5001")
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			responseBodyBytes, err := utils.ReadByte(conn)
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Error.Println("Client network io process decode server response failed, err: ", err)
				return
			}
			response := &pkgrpc.MessageResponse{}
			_ = utils.Decode(responseBodyBytes, response)
			log.Info.Println("Receive to server response: ", utils.ToJson(response))
		}
	}()
	//
	//_, err = conn.Write(utils.Encode(FetchServerNodeIdRequest))
	//if err != nil {
	//	fmt.Println("write error, err: ", err)
	//}
	//response := processRequestTest(conn)
	//
	//fetchServerNodeIdResponse := pkgrpc.FetchServerNodeIdResponse{}
	//utils.Decode(response.GetResult().GetData(), &fetchServerNodeIdResponse)
	//
	//nodeId := fetchServerNodeIdResponse.GetServerNodeId()
	//nodeMap := make(map[int32]string)
	//nodeMap[1] = "127.0.0.1:5000"
	//nodeMap[2] = "127.0.0.1:5001"
	//if nodeId != 3 {
	//	connTemp, err := net.Dial("tcp", nodeMap[nodeId])
	//	if err != nil {
	//		panic(err)
	//	}
	//	conn = connTemp
	//}

	_, err = conn.Write(utils.Encode(heartbeatRequest))
	if err != nil {
		fmt.Println("write error, err: ", err)
	}
	processRequestTest(conn)

	for {
		time.Sleep(time.Second)
	}
}

func processRequestTest(conn net.Conn) *pkgrpc.MessageResponse {
	responseBodyBytes, err := utils.ReadByte(conn)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Error.Println("Client network io process decode server response failed, err: ", err)
		return nil
	}
	response := &pkgrpc.MessageResponse{}
	_ = utils.Decode(responseBodyBytes, response)
	log.Info.Println("Receive to server response: ", utils.ToJson(response))
	return response
}
