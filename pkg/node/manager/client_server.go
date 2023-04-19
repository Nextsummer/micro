package manager

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/config"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/google/uuid"
	cmap "github.com/orcaman/concurrent-map/v2"
	"io"
	"net"
	"strings"
	"sync"
)

func StartClientIO() {
	clientTcpPort := config.GetConfigurationInstance().NodeClientTcpPort
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", clientTcpPort))
	if err != nil {
		log.Error.Fatalf("Client io server binding to port [%d] error, error msg: %v", clientTcpPort, err)
	}
	go networkIO(listen)
}

func networkIO(listen net.Listener) {
	defer listen.Close()

	for IsRunning() {
		conn, err := listen.Accept()
		if err != nil {
			log.Error.Println("Client io server accept failed, err: ", err)
			continue
		}
		go process(conn)
	}
}

func process(conn net.Conn) {
	defer func(conn net.Conn) {
		if err := recover(); err != nil {
			log.Error.Println("Client io server conn process error, err: ", err)
			conn.Close()
		}
	}(conn)

	connection := NewClientConnection(conn)
	GetClientConnectionManagerInstance().clientConnections.Put(connection)
	GetClientMessageQueuesInstance().initMessageQueue(connection.ConnectionId)

	log.Info.Println("Establishing a connection with the client: ", conn.RemoteAddr())
	go func() {
		for IsRunning() {
			requestBodyBytes, err := utils.ReadByte(conn)
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Error.Println("Client io server connection client exception, err: ", err)
				return
			}
			message := &pkgrpc.MessageEntity{}
			_ = utils.Decode(requestBodyBytes, message)
			log.Info.Println("Receive to client message: ", utils.ToJson(message))
			response := processRequest(connection.ConnectionId, message)
			GetClientMessageQueuesInstance().putMessage(connection.ConnectionId, response)
		}
	}()

	go func() {
		for IsRunning() {
			messageQueue, ok := GetClientMessageQueuesInstance().messageQueues.Get(connection.ConnectionId)
			if !ok {
				continue
			}
			for {
				response, ok := messageQueue.Take()
				if !ok {
					break
				}
				_, err := conn.Write(utils.Encode(&response))
				log.Info.Println("Write response data to client: ", utils.ToJson(response))
				if err != nil {
					log.Error.Println("Client io server process write response failed, err: ", err)
					return
				}
			}
		}
	}()
}

type ClientConnection struct {
	HasReadMessage bool
	ConnectionId   string
	Conn           net.Conn
}

func NewClientConnection(conn net.Conn) *ClientConnection {
	return &ClientConnection{
		Conn:         conn,
		ConnectionId: strings.ReplaceAll(uuid.New().String(), "-", ""),
	}
}

var clientConnectionManagerOnce sync.Once
var clientConnectionManager *ClientConnectionManager

type ClientConnectionManager struct {
	clientConnections queue.Array[*ClientConnection]
}

func GetClientConnectionManagerInstance() *ClientConnectionManager {
	clientConnectionManagerOnce.Do(func() {
		clientConnectionManager = &ClientConnectionManager{*queue.NewArray[*ClientConnection]()}
	})
	return clientConnectionManager
}

var clientMessageQueuesOnce sync.Once
var clientMessageQueues *ClientMessageQueues

type ClientMessageQueues struct {
	messageQueues cmap.ConcurrentMap[string, *queue.Array[pkgrpc.MessageResponse]]
}

func GetClientMessageQueuesInstance() *ClientMessageQueues {
	clientMessageQueuesOnce.Do(func() {
		clientMessageQueues = &ClientMessageQueues{cmap.New[*queue.Array[pkgrpc.MessageResponse]]()}
	})
	return clientMessageQueues
}

func (c *ClientMessageQueues) initMessageQueue(clientConnectionId string) {
	c.messageQueues.Set(clientConnectionId, queue.NewArray[pkgrpc.MessageResponse]())
}

func (c *ClientMessageQueues) putMessage(clientConnectionId string, message *pkgrpc.MessageResponse) {
	messageQueue, _ := c.messageQueues.Get(clientConnectionId)
	messageQueue.Put(*message)
}

type ClientRequestProcessor struct {
}
