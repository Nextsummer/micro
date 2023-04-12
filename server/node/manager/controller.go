package manager

import (
	"fmt"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/Nextsummer/micro/server/config"
	"github.com/Nextsummer/micro/server/node/persist"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
	"time"
)

const (
	SlotsCount                     = 16384                      // slot槽位的总数量
	SlotsAllocationFilename        = "slots_allocation"         // 槽位分配存储文件的名字
	SlotsReplicaAllocationFilename = "slots_replica_allocation" // 槽位分配存储文件的名字
	ReplicaNodeIdsFilename         = "replica_node_ids"
	NodeSlotsFilename              = "node_slots" // 槽位分配存储文件的名字
	NodeSlotsReplicasFilename      = "node_slots_replicas"
)

var controllerOnce sync.Once
var controllerInstance *Controller

type Controller struct {
	slotsAllocation        cmap.ConcurrentMap[int32, *queue.Array[string]]
	slotsReplicaAllocation cmap.ConcurrentMap[int32, *queue.Array[string]]
	replicaNodeIds         cmap.ConcurrentMap[int32, int32]
	startTimestamp         int64
}

func getControllerInstance() *Controller {
	controllerOnce.Do(func() {
		controllerInstance = &Controller{
			slotsAllocation:        cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode),
			slotsReplicaAllocation: cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](Int32HashCode),
			replicaNodeIds:         cmap.NewWithCustomShardingFunction[int32, int32](Int32HashCode),
			startTimestamp:         time.Now().Unix(),
		}
	})
	return controllerInstance
}

// Assign slots to all master machines
func (c *Controller) allocateSlots() {
	c.executeSlotsAllocation()

	// For the slot range for which each node is responsible, the copy of the slot range is calculated to assign to the other node.
	// Let's say you have four nodes, and each node is allocated a slot range.
	// For node 1, its slot range copy is randomly selected among the other three nodes, node 234, and so on.
	// A copy of the slot range is allocated and, once calculated, persisted on local disk and synchronized to another master candidate.
	c.executeSlotsReplicaAllocation()

	// Write slot assignment data to the local disk file
	if !c.persistSlotsAllocation() || !c.persistSlotsReplicaAllocation() || !c.persistReplicaNodeIds() {
		Fatal()
		return
	}

	// The slots are allocated and persisted in the Controller's own memory as well as on disk.
	// The Controller is responsible for sending slot allocation data to other Controller candidates.
	// Other Controller candidates need to maintain a copy of slot allocation data in their own memory and persist it to disk.
	c.syncSlotsAllocation()
	c.syncSlotsReplicaAllocation()
	c.syncReplicaNodeIds()

	// The Controller initializes the slot data internally.
	c.initSlots()
	// The copy of the slot range for which you are responsible is initialized in memory and also persisted to disk.
	c.initSlotsReplicas()
	// Tell each node which slot range it is responsible for, and which other nodes have a copy of that slot range.
	c.initReplicaNodeId()

	// In addition to sending the complete slot allocation data to the other candidates,
	// He needs to send each master their respective slot range, and each master does a slot initialization in memory,
	// You also need to do a persistence on the local disk.
	c.sendNodeSlots()
	// The node initializes its own range of slots in memory,
	// It also initializes and persists a copy of the slot scope for which it is responsible.
	c.sendNodeSlotsReplicas()
	c.sendReplicaNodeId()

}

// Initialize controller node data
func (c *Controller) initControllerNode() {
	SetControllerNodeId(config.GetConfigurationInstance().NodeId)
}

// Calculate slot assignment data
func (c *Controller) executeSlotsAllocation() {
	nodeId := config.GetConfigurationInstance().NodeId
	remoteMasterNodes := GetRemoteServerNodeManagerInstance().getRemoteServerNodes()

	totalMasterNodeCount := len(remoteMasterNodes) + 1
	// Calculate how many slots each master node is allocated to on average
	slotsPerMasterNode := SlotsCount / totalMasterNodeCount
	// Calculate the number of slots allocated to the controller, and add more if any
	remainSlotsCount := SlotsCount - slotsPerMasterNode*totalMasterNodeCount

	// Initializes the number of slots for each master node
	nextStartSlot := 1
	nextEndSlot := nextStartSlot - 1 + slotsPerMasterNode

	for _, remoteMasterNode := range remoteMasterNodes {
		slotsList := queue.NewArray[string]()
		slotsList.Put(fmt.Sprintf("%d,%d", nextStartSlot, nextEndSlot))
		c.slotsAllocation.Set(remoteMasterNode.GetNodeId(), slotsList)
		nextStartSlot = nextEndSlot + 1
		nextEndSlot = nextStartSlot - 1 + slotsPerMasterNode
	}

	slotsList := queue.NewArray[string]()
	slotsList.Put(fmt.Sprintf("%d,%d", nextStartSlot, nextEndSlot+remainSlotsCount))
	c.slotsAllocation.Set(nodeId, slotsList)
	log.Info.Println("Receive slots allocated data : ", utils.ToJson(c.slotsAllocation))
}

// Perform the assignment of a copy of slots
func (c *Controller) executeSlotsReplicaAllocation() {
	nodeIds := queue.NewArray[int32]()
	nodeId := config.GetConfigurationInstance().NodeId
	nodeIds.Put(nodeId)

	remoteMasterNodes := GetRemoteServerNodeManagerInstance().getRemoteServerNodes()
	for _, remoteMasterNode := range remoteMasterNodes {
		nodeIds.Put(remoteMasterNode.GetNodeId())
	}

	// Perform the assignment of a copy of slots
	for nodeSlots := range c.slotsAllocation.IterBuffered() {
		nodeId := nodeSlots.Key
		slots := nodeSlots.Val

		var replicaNodeId int32
		hasDecidedReplicaNode := false
		for !hasDecidedReplicaNode {
			replicaNodeId = nodeIds.RandomTake()
			if nodeId != replicaNodeId {
				hasDecidedReplicaNode = true
			}
		}
		slotsReplicas, ok := c.slotsReplicaAllocation.Get(replicaNodeId)
		if !ok {
			slotsReplicasTemp := queue.NewArray[string]()
			slotsReplicas = slotsReplicasTemp
			c.slotsReplicaAllocation.Set(replicaNodeId, slotsReplicas)
		}
		slotsReplicas.PutAll(slots.Iter())
		c.replicaNodeIds.Set(nodeId, replicaNodeId)
	}
	log.Info.Println("Receive slots replica allocated data: ", utils.ToJson(c.slotsReplicaAllocation))
}

func (c *Controller) persistSlotsAllocation() bool {
	return persist.Persist(utils.ToJsonByte(c.slotsAllocation), SlotsAllocationFilename)
}

func (c *Controller) persistSlotsReplicaAllocation() bool {
	return persist.Persist(utils.ToJsonByte(c.slotsReplicaAllocation), SlotsReplicaAllocationFilename)
}

func (c *Controller) persistReplicaNodeIds() bool {
	return persist.Persist(utils.ToJsonByte(c.replicaNodeIds), ReplicaNodeIdsFilename)
}

// Synchronize slots to allocate data to other controller candidates
func (c *Controller) syncSlotsAllocation() {
	slotsAllocationBytes := utils.ToJsonByte(c.slotsAllocation)
	for _, controllerCandidate := range GetRemoteServerNodeManagerInstance().getOtherControllerCandidates() {
		GetServerNetworkManagerInstance().sendMessage(controllerCandidate.GetNodeId(),
			pkgrpc.MessageEntity_SLOTS_ALLOCATION, slotsAllocationBytes)
	}

	log.Info.Println("Slot assignment data is synchronized to controller candidate nodes!")
}

func (c *Controller) syncSlotsReplicaAllocation() {
	replicaNodeIdsBytes := utils.ToJsonByte(c.replicaNodeIds)
	for _, controllerCandidate := range GetRemoteServerNodeManagerInstance().getOtherControllerCandidates() {
		GetServerNetworkManagerInstance().sendMessage(controllerCandidate.GetNodeId(),
			pkgrpc.MessageEntity_REPLICA_NODE_IDS, replicaNodeIdsBytes)
	}
	log.Info.Println("The replica node id set is synchronized to the controller candidate node!")
}

func (c *Controller) syncReplicaNodeIds() {
	slotsReplicaAllocationBytes := utils.ToJsonByte(c.slotsReplicaAllocation)
	for _, controllerCandidate := range GetRemoteServerNodeManagerInstance().getOtherControllerCandidates() {
		GetServerNetworkManagerInstance().sendMessage(controllerCandidate.GetNodeId(),
			pkgrpc.MessageEntity_SLOTS_REPLICA_ALLOCATION, slotsReplicaAllocationBytes)
	}
	log.Info.Println("The slot copy allocates data to the controller candidate node!")
}

// The slots for which the controller is responsible are initialized in memory
func (c *Controller) initSlots() {
	slotsArray, _ := c.slotsAllocation.Get(config.GetConfigurationInstance().NodeId)
	GetSlotManagerInstance().initSlots(slotsArray)
}

// The controller's own copy of the slot is initialized in memory
func (c *Controller) initSlotsReplicas() {
	slotsReplicas, _ := c.slotsReplicaAllocation.Get(config.GetConfigurationInstance().NodeId)
	GetSlotManagerInstance().initSlotsReplicas(slotsReplicas, true)
}

// Initializes the controller's own copy of the target slot
func (c *Controller) initReplicaNodeId() {
	replicaNodeId, _ := c.replicaNodeIds.Get(config.GetConfigurationInstance().NodeId)
	GetSlotManagerInstance().initReplicaNodeId(replicaNodeId)
}

// Send to each master node the range of slots they are responsible for.
func (c *Controller) sendNodeSlots() {
	for _, node := range GetRemoteServerNodeManagerInstance().getRemoteServerNodes() {
		slots, ok := c.slotsAllocation.Get(node.GetNodeId())
		if !ok {
			continue
		}
		GetServerNetworkManagerInstance().sendMessage(node.GetNodeId(), pkgrpc.MessageEntity_NODE_SLOTS, utils.ToJsonByte(slots))
	}
	log.Info.Println("Sending the slot range to other nodes is complete.")
}

func (c *Controller) sendNodeSlotsReplicas() {
	for _, node := range GetRemoteServerNodeManagerInstance().getRemoteServerNodes() {
		slotsReplicas, ok := c.slotsReplicaAllocation.Get(node.GetNodeId())
		if !ok {
			slotsReplicas = queue.NewArray[string]()
		}
		GetServerNetworkManagerInstance().sendMessage(node.GetNodeId(),
			pkgrpc.MessageEntity_NODE_SLOTS_REPLICAS, utils.ToJsonByte(slotsReplicas))
	}
	log.Info.Println("Sending slot copies to other nodes is complete.")
}

func (c *Controller) sendReplicaNodeId() {
	for _, node := range GetRemoteServerNodeManagerInstance().getRemoteServerNodes() {
		replicaNodeId, ok := c.replicaNodeIds.Get(node.GetNodeId())
		if !ok {
			continue
		}
		GetServerNetworkManagerInstance().sendMessage(node.GetNodeId(),
			pkgrpc.MessageEntity_REPLICA_NODE_ID, utils.ToJsonByte(replicaNodeId))
	}
	log.Info.Println("Sending the replica node id to another node is complete.")
}

func sendControllerNodeId() {
	for _, node := range GetRemoteServerNodeManagerInstance().getRemoteServerNodes() {
		GetServerNetworkManagerInstance().sendMessage(node.GetNodeId(),
			pkgrpc.MessageEntity_CONTROLLER_NODE_ID, utils.ToJsonByte(config.GetConfigurationInstance().NodeId))
	}
	log.Info.Println("Sending controller node ids to all nodes is complete.")
}

func (c *Controller) syncSlotsAllocationToCandidateNodeId(candidateNodeId int32) {
	slotsAllocationBytes := utils.ToJsonByte(c.slotsAllocation)
	GetServerNetworkManagerInstance().sendMessage(candidateNodeId,
		pkgrpc.MessageEntity_SLOTS_ALLOCATION, slotsAllocationBytes)
}
