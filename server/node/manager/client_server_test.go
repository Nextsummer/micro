package manager

import (
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/Nextsummer/micro/server/config"
	"io"
	"net"
	"testing"
	"time"
)

func TestStartClientIO(t *testing.T) {

	log.InitLog("/temp")
	Running()
	config.GetConfigurationInstance().NodeClientTcpPort = 30000
	StartClientIO()

	for {
		time.Sleep(time.Second)

	}
}

func TestClientConnection(t *testing.T) {
	log.InitLog("/temp")

	//array := queue.NewArray[net.Conn]()
	for i := 0; i < 1000; i++ {
		go connectionClient()
	}

	for {
		time.Sleep(time.Second)

	}

	//time.Sleep(time.Second)
	//for _, a := range array.ClearAndIter() {
	//	a.Close()
	//
	//}
}

func connectionClient() net.Conn {
	conn, err := net.Dial("tcp", "127.0.0.1:30000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	registerRequest := &pkgrpc.RegisterRequest{ServiceName: "hello"}
	request := &pkgrpc.MessageEntity{
		RequestId: "1",
		Type:      pkgrpc.MessageEntity_CLIENT_REGISTER,
		Data:      utils.Encode(registerRequest),
	}

	_, err = conn.Write(utils.Encode(request))
	if err != nil {
		panic(err)
	}

	for {
		responseBodyBytes, err := utils.ReadByte(conn)
		if err == io.EOF {
			return conn
		}
		if err != nil {
			log.Error.Println("Client io server process decode message failed, err: ", err)
			return conn
		}

		response := &pkgrpc.MessageResponse{}
		_ = utils.Decode(responseBodyBytes, response)
		log.Info.Println("Receive to server message: ", utils.ToJson(response))
		break
	}
	return conn
}
