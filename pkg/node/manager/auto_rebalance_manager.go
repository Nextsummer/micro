package manager

import (
	"github.com/Nextsummer/micro/pkg/queue"
	"math/rand"
	"time"
)

const (
	AutoRebalanceThreshold = 5 * 60
)

// rebalance
func rebalance(joinedNodeId int32) {
	controller := getControllerInstance()
	startTimestamp := controller.startTimestamp

	if time.Now().Unix()-startTimestamp > AutoRebalanceThreshold {
		slotsAllocation := controller.slotsAllocation
		replicaNodeIds := controller.replicaNodeIds

		rand.Seed(time.Now().Unix())
		randIndex := rand.Intn(len(slotsAllocation.Keys()))

		currentIndex := 0
		var selectedNodeId int32
		//var selectedNodeSlots *queue.Array[string]
		selectedNodeSlots := queue.NewArray[string]()

		for _, nodeId := range slotsAllocation.Keys() {
			if currentIndex == randIndex {
				selectedNodeId = nodeId
				selectedNodeSlots, _ = slotsAllocation.Get(nodeId)
			}
			currentIndex++
		}

		if selectedNodeSlots.Size() > 1 {
			selectedSlots := selectedNodeSlots.RandomTake()
			joinedNodeSlots := queue.NewArray[string]()
			joinedNodeSlots.Put(selectedSlots)

			slotsAllocation.Set(joinedNodeId, joinedNodeSlots)

			replicaNodeId, _ := replicaNodeIds.Get(selectedNodeId)
			replicaNodeIds.Set(joinedNodeId, replicaNodeId)

			controller.syncSlotsAllocation()
			controller.syncReplicaNodeIds()

			controller.sendNodeSlotsToNodeId(joinedNodeId)
			controller.sendReplicaNodeIdToNodeId(joinedNodeId)
		}
	}
}
