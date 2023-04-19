package manager

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/config"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
	"time"
)

const (
	CandidateReconnectInterval = 60
)

var highAvailabilityManagerOnce sync.Once
var highAvailabilityManager *HighAvailabilityManager

// HighAvailabilityManager High availability management component
// Scalable architecture, a controller or candidate crashed, and you added a new node,
// Check whether it is a controller candidate. If so, it should be automatically added
// to the controller candidate cluster and the cluster metadata should be automatically synchronized.
// If it is an ordinary node, then it is automatic to do the data rebalance,
// automatically do a recalculation of slot data, do a rebalancing.
type HighAvailabilityManager struct {
	disconnectedNodeQueue *queue.Array[int32]
}

func GetHighAvailabilityManagerInstance() *HighAvailabilityManager {
	highAvailabilityManagerOnce.Do(func() {
		highAvailabilityManager = &HighAvailabilityManager{queue.NewArray[int32]()}
		go highAvailabilityManager.worker()
	})
	return highAvailabilityManager
}

// Handle network disconnection from remote node exception
func (h *HighAvailabilityManager) handleDisconnectedException(remoteNodeId int32) {
	h.disconnectedNodeQueue.Put(remoteNodeId)
}

func (h *HighAvailabilityManager) worker() {
	for IsRunning() {
		myNodeId := config.GetConfigurationInstance().NodeId
		controllerNode := GetControllerNodeInstance()
		controllerCandidate := GetControllerCandidateInstance()
		disconnectedNodeId, ok := h.disconnectedNodeQueue.Take()
		if !ok {
			continue
		}

		remoteServerNodeManager := GetRemoteServerNodeManagerInstance()
		remoteServerNode, ok := remoteServerNodeManager.getRemoteServerNode(disconnectedNodeId)
		if !ok {
			continue
		}
		isDisconnectedCandidate := remoteServerNode.GetIsControllerCandidate()
		disconnectedAddress := fmt.Sprintf("%s:%d", remoteServerNode.GetIp(), remoteServerNode.GetClientPort())

		releaseNetworkResource(disconnectedNodeId)

		// Check whether the connection to the controller is disconnected.
		// If the controller crashes and you are the candidate node, you must enable the high availability mechanism to initiate a controller re-election.
		if isCandidate() && controllerNode.IsControllerNode(disconnectedNodeId) {
			serverNodeRole := controllerCandidate.electController()
			SetServerNodeRole(serverNodeRole)
			log.Info.Printf("Controller Node the result after the re-election: %s", getServerNodeRole(serverNodeRole))

			if serverNodeRole == controller {
				recalculateAllocation(disconnectedNodeId)
				resyncAllocation()
			} else if serverNodeRole == candidate {
				// If it's the controller candidate.
				// All slots and slot replicas need to be rereceived.
				controllerCandidate.waitForSlotsAllocation()
				controllerCandidate.waitForSlotsReplicaAllocation()
				controllerCandidate.waitReplicaNodeIds()
			}
			if myNodeId > disconnectedNodeId {
				NewCandidateReconnectAndRun(disconnectedNodeId, disconnectedAddress)
			}
		} else if IsController() {
			recalculateAllocation(disconnectedNodeId)
			// In the process of recalculation, the newly calculated data needs to be synchronized to the corresponding candidate and common node.
			resyncAllocation()

			if isDisconnectedCandidate {
				if myNodeId > disconnectedNodeId {
					NewCandidateReconnectAndRun(disconnectedNodeId, disconnectedAddress)
				}
			}
		} else if isCandidate() && !controllerNode.IsControllerNode(disconnectedNodeId) {
			controllerCandidate.waitForSlotsAllocation()
			controllerCandidate.waitForSlotsReplicaAllocation()
			controllerCandidate.waitReplicaNodeIds()

			if isDisconnectedCandidate {
				if myNodeId > disconnectedNodeId {
					NewCandidateReconnectAndRun(disconnectedNodeId, disconnectedAddress)
				}
			}
		} else if isCommonNode() && isDisconnectedCandidate {
			NewCandidateReconnectAndRun(disconnectedNodeId, disconnectedAddress)
		}

	}
}

// CandidateReconnect The candidate node reconnects
type CandidateReconnect struct {
	candidateNodeId  int32
	candidateAddress string
}

func NewCandidateReconnectAndRun(candidateNodeId int32, candidateAddress string) {
	c := &CandidateReconnect{
		candidateNodeId:  candidateNodeId,
		candidateAddress: candidateAddress,
	}
	go c.run()
}

func (c *CandidateReconnect) run() {
	for IsRunning() {
		// Every once in a while, try to re-connect a lost candidate with a smaller nodeid than you
		time.Sleep(time.Second * time.Duration(CandidateReconnectInterval))
		isConnected := connectServerNode(c.candidateAddress)
		if isConnected {
			break
		}
	}
}

// Resynchronize the allocation of data to the other controller candidate node
func resyncAllocation() {
	controller := getControllerInstance()
	controller.syncSlotsAllocation()
	controller.syncReplicaNodeIds()
	controller.syncSlotsReplicaAllocation()
}

func releaseNetworkResource(disconnectedNodeId int32) {
	networkManager := GetServerNetworkManagerInstance()
	networkManager.shutdownIO(disconnectedNodeId)
	networkManager.removeRemoteNodeClientStream(disconnectedNodeId)

	GetRemoteServerNodeManagerInstance().removeServerNode(disconnectedNodeId)
}

// Perform a recalculation of the allocated data
func recalculateAllocation(disconnectedNodeId int32) {
	controllerCandidate := GetControllerCandidateInstance()
	slotsAllocation := controllerCandidate.slotsAllocation
	replicaNodeIds := controllerCandidate.replicaNodeIds

	// 1. Recalculate slot assignment data
	disconnectedSlots, ok := slotsAllocation.Get(disconnectedNodeId)
	if !ok {
		return
	}
	replicaNodeId, ok := replicaNodeIds.Get(disconnectedNodeId)
	if !ok {
		return
	}
	replicaNodeSlots, ok := slotsAllocation.Get(replicaNodeId)
	if !ok {
		return
	}

	slotsAllocation.Remove(disconnectedNodeId)
	replicaNodeSlots.PutAll(disconnectedSlots.Iter())

	// Direct is to tell the replica node to correct the slot copy above it.
	changeReplicaToSlots(replicaNodeId, disconnectedSlots)

	// 2. The replica node id is recalculated
	nodeIds := queue.NewArray[int32]()
	remoteServerNodes := GetRemoteServerNodeManagerInstance().getRemoteServerNodes()
	for _, remoteServerNode := range remoteServerNodes {
		nodeIds.Put(remoteServerNode.GetNodeId())
	}
	nodeIds.Remove(disconnectedNodeId)

	newReplicaNodeIds := cmap.NewWithCustomShardingFunction[int32, int32](utils.Int32HashCode)
	for _, nodeId := range replicaNodeIds.Keys() {
		oldReplicaNodeId, ok := replicaNodeIds.Get(nodeId)
		if !ok {
			continue
		}
		if oldReplicaNodeId == disconnectedNodeId {
			var newReplicaNodeId int32
			for {
				newReplicaNodeId = nodeIds.RandomTake()
				if newReplicaNodeId != nodeId {
					break
				}
			}
			newReplicaNodeIds.Set(nodeId, newReplicaNodeId)
		}
	}

	for _, nodeId := range newReplicaNodeIds.Keys() {
		newReplicaNodeId, ok := newReplicaNodeIds.Get(nodeId)
		if !ok {
			continue
		}
		replicaNodeIds.Set(nodeId, newReplicaNodeId)
	}
	replicaNodeIds.Remove(disconnectedNodeId)

	// Synchronize the changed replica node id to the corresponding node for reset.
	slotsReplicaAllocation := cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](utils.Int32HashCode)
	for _, nodeId := range replicaNodeIds.Keys() {
		replicaNodeId, ok := replicaNodeIds.Get(nodeId)
		if !ok {
			continue
		}
		slots, ok := slotsAllocation.Get(nodeId)
		if !ok {
			continue
		}
		slotsReplicaAllocation.Set(replicaNodeId, slots)
		refreshReplicaSlots(replicaNodeId, slots)
	}
	log.Info.Println("Recalculated slot allocation data: ", utils.ToJson(slotsAllocation))
	log.Info.Println("id of the replica node after recalculation: ", utils.ToJson(replicaNodeIds))
	log.Info.Println("Recalculated slot replica data: ", utils.ToJson(slotsReplicaAllocation))

	controller := getControllerInstance()
	controller.setSlotsAllocation(slotsAllocation)
	controller.setReplicaNodeIds(replicaNodeIds)
	controller.setSlotsReplicaAllocation(slotsReplicaAllocation)
}

func changeReplicaToSlots(nodeId int32, slots *queue.Array[string]) {
	myNodeId := config.GetConfigurationInstance().NodeId

	if nodeId == myNodeId {
		GetSlotManagerInstance().changeReplicaToSlots(slots)
		return
	}
	GetServerNetworkManagerInstance().sendMessage(nodeId,
		pkgrpc.MessageEntity_CHANGE_REPLICA_TO_SLOTS, utils.ToJsonByte(slots))
}

func refreshReplicaSlots(nodeId int32, replicaSlots *queue.Array[string]) {
	myNodeId := config.GetConfigurationInstance().NodeId

	if nodeId == myNodeId {
		GetSlotManagerInstance().refreshReplicaSlots(replicaSlots)
		return
	}
	GetServerNetworkManagerInstance().sendMessage(nodeId,
		pkgrpc.MessageEntity_REFRESH_REPLICA_SLOTS, utils.ToJsonByte(replicaSlots))
}
