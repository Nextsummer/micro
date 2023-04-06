package manager

import (
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"sync"
)

// ServerMessageReceiver Master node's message receiving component (coroutine)
// 1.Constantly getting the latest messages from the receiver queue of the network communication component
// 2.Determine the type of the message and convert the message to an object
// 3.Push messages to queues corresponding to different business modules
// 4.Provides interfaces for various business modules to get their own business messages
type ServerMessageReceiver struct {
	voteReceiveQueue queue.Array[*pkgrpc.ControllerVote] // 投票消息接收队列
}

var serverMessageReceiverOnce sync.Once
var serverMessageReceiver *ServerMessageReceiver

func getServerMessageReceiverInstance() *ServerMessageReceiver {
	serverMessageReceiverOnce.Do(func() {
		serverMessageReceiver = &ServerMessageReceiver{
			voteReceiveQueue: *queue.NewArray[*pkgrpc.ControllerVote](),
		}
	})
	return serverMessageReceiver
}

func (s *ServerMessageReceiver) run() {
	networkManager := GetServerNetworkManagerInstance()

	for IsRunning() {
		message, ok := networkManager.receiveQueue.Take()
		if !ok {
			continue
		}
		if pkgrpc.MessageEntity_VOTE == message.GetType() {
			controllerVote := &pkgrpc.ControllerVote{}
			utils.Decode(message.GetData(), controllerVote)
			s.voteReceiveQueue.Put(controllerVote)
			log.Info.Println("A controller vote was received: ", utils.ToJson(controllerVote))
		}
	}
}
