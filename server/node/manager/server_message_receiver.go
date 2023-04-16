package manager

import (
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
)

// ServerMessageReceiver Master node's message receiving component (coroutine)
// 1.Constantly getting the latest messages from the receiver queue of the network communication component
// 2.Determine the type of the message and convert the message to an object
// 3.Push messages to queues corresponding to different business modules
// 4.Provides interfaces for various business modules to get their own business messages
type ServerMessageReceiver struct {
	// Vote message receiving queue
	voteReceiveQueue queue.Array[*pkgrpc.ControllerVote]
	// Slot data receiving queue
	slotsAllocationReceiveQueue        queue.Array[cmap.ConcurrentMap[int32, *queue.Array[string]]]
	slotsReplicaAllocationReceiveQueue queue.Array[cmap.ConcurrentMap[int32, *queue.Array[string]]]
	replicaNodeIdsQueue                queue.Array[cmap.ConcurrentMap[int32, int32]]

	// The message receiving queue for the slot range for which you are responsible
	nodeSlotsQueue         queue.Array[queue.Array[string]]
	nodeSlotsReplicasQueue queue.Array[queue.Array[string]]
	replicaNodeIdQueue     queue.Array[int32]
	controllerNodeIdQueue  queue.Array[int32]
	RegisterRequestQueue   queue.Array[pkgrpc.RegisterRequest]
	HeartbeatRequestQueue  queue.Array[pkgrpc.HeartbeatRequest]
}

var serverMessageReceiverOnce sync.Once
var serverMessageReceiver *ServerMessageReceiver

func GetServerMessageReceiverInstance() *ServerMessageReceiver {
	serverMessageReceiverOnce.Do(func() {
		serverMessageReceiver = &ServerMessageReceiver{
			*queue.NewArray[*pkgrpc.ControllerVote](),
			*queue.NewArray[cmap.ConcurrentMap[int32, *queue.Array[string]]](),
			*queue.NewArray[cmap.ConcurrentMap[int32, *queue.Array[string]]](),
			*queue.NewArray[cmap.ConcurrentMap[int32, int32]](),
			*queue.NewArray[queue.Array[string]](),
			*queue.NewArray[queue.Array[string]](),
			*queue.NewArray[int32](),
			*queue.NewArray[int32](),
			*queue.NewArray[pkgrpc.RegisterRequest](),
			*queue.NewArray[pkgrpc.HeartbeatRequest](),
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
		messageType := message.GetType()
		data := message.GetData()
		if pkgrpc.MessageEntity_VOTE == messageType {
			controllerVote := &pkgrpc.ControllerVote{}
			_ = utils.Decode(data, controllerVote)
			s.voteReceiveQueue.Put(controllerVote)
			log.Info.Println("A controller vote was received: ", utils.ToJson(controllerVote))
		} else if pkgrpc.MessageEntity_SLOTS_ALLOCATION == messageType {
			slotsAllocation := cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode)
			utils.BytesToJson(data, &slotsAllocation)
			s.slotsAllocationReceiveQueue.Put(slotsAllocation)
			log.Info.Println("Receive slot allocation data: ", utils.ToJson(slotsAllocation))
		} else if pkgrpc.MessageEntity_NODE_SLOTS == messageType {
			slots := queue.NewArray[string]()
			utils.BytesToJson(data, &slots)
			s.nodeSlotsQueue.Put(*slots)
			log.Info.Println("The slot range for this node is received: ", utils.ToJson(slots))
		} else if pkgrpc.MessageEntity_SLOTS_REPLICA_ALLOCATION == messageType {
			slotsReplicaAllocationReceive := cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode)
			utils.BytesToJson(data, &slotsReplicaAllocationReceive)
			s.slotsReplicaAllocationReceiveQueue.Put(slotsReplicaAllocationReceive)
			log.Info.Println("Received slots replica allocation data: ", utils.ToJson(slotsReplicaAllocationReceive))
		} else if pkgrpc.MessageEntity_NODE_SLOTS_REPLICAS == messageType {
			slotsReplicas := queue.NewArray[string]()
			utils.BytesToJson(data, &slotsReplicas)
			s.nodeSlotsReplicasQueue.Put(*slotsReplicas)
			log.Info.Println("The slot replica set for this node is received: ", utils.ToJson(slotsReplicas))
		} else if pkgrpc.MessageEntity_REPLICA_NODE_ID == messageType {
			var replicaNodeId int32
			utils.BytesToJson(data, &replicaNodeId)
			s.replicaNodeIdQueue.Put(replicaNodeId)
			log.Info.Println("Received replica node id: ", replicaNodeId)
		} else if pkgrpc.MessageEntity_REPLICA_REGISTER == messageType {
			registerRequest := pkgrpc.RegisterRequest{}
			utils.Decode(data, &registerRequest)
			s.RegisterRequestQueue.Put(registerRequest)
			log.Info.Println("Received service registration request forwarded to replica: ", utils.ToJson(registerRequest))
		} else if pkgrpc.MessageEntity_REPLICA_HEARTBEAT == messageType {
			heartbeatRequest := pkgrpc.HeartbeatRequest{}
			utils.Decode(data, &heartbeatRequest)
			s.HeartbeatRequestQueue.Put(heartbeatRequest)
			log.Info.Println("Received a service heartbeat request forwarded to the replica: ", utils.ToJson(heartbeatRequest))
		} else if pkgrpc.MessageEntity_REPLICA_NODE_IDS == messageType {
			replicaNodeIds := cmap.NewWithCustomShardingFunction[int32, int32](Int32HashCode)
			utils.BytesToJson(data, &replicaNodeIds)
			s.replicaNodeIdsQueue.Put(replicaNodeIds)
			log.Info.Println("Received replica node id collection: ", utils.ToJson(replicaNodeIds))
		} else if pkgrpc.MessageEntity_CONTROLLER_NODE_ID == messageType {
			var controllerNodeId int32
			utils.BytesToJson(data, &controllerNodeId)
			s.controllerNodeIdQueue.Put(controllerNodeId)
			log.Info.Println("Received controller node id: ", controllerNodeId)
		} else if pkgrpc.MessageEntity_CHANGE_REPLICA_TO_SLOTS == messageType {
			slots := queue.NewArray[string]()
			utils.BytesToJson(data, &slots)
			GetSlotManagerInstance().changeReplicaToSlots(slots)
		} else if pkgrpc.MessageEntity_REFRESH_REPLICA_NODE_ID == messageType {
			var newReplicaNodeId int32
			utils.BytesToJson(data, &newReplicaNodeId)
			GetSlotManagerInstance().refreshReplicaNodeId(newReplicaNodeId)
		} else if pkgrpc.MessageEntity_REFRESH_REPLICA_SLOTS == messageType {
			replicaSlots := queue.NewArray[string]()
			utils.BytesToJson(data, &replicaSlots)
			GetSlotManagerInstance().refreshReplicaSlots(replicaSlots)
		} else if pkgrpc.MessageEntity_REQUEST_SLOTS_DATA == messageType {
			var candidateNodeId int32
			utils.BytesToJson(data, &candidateNodeId)
			controller := getControllerInstance()
			controller.syncSlotsAllocationToCandidateNodeId(candidateNodeId)
			controller.syncSlotsReplicaAllocationToCandidateNodeId(candidateNodeId)
			controller.syncReplicaNodeIdsToCandidateNodeId(candidateNodeId)
		} else if pkgrpc.MessageEntity_UPDATE_NODE_SLOTS == messageType {
			slotsList := queue.NewArray[string]()
			utils.BytesToJson(data, &slotsList)
			GetSlotManagerInstance().initSlots(slotsList)
		} else if pkgrpc.MessageEntity_UPDATE_REPLICA_NODE_ID == messageType {

		} else if pkgrpc.MessageEntity_TRANSFER_SLOTS == messageType {

		} else if pkgrpc.MessageEntity_UPDATE_SLOTS == messageType {

		}
	}
}
