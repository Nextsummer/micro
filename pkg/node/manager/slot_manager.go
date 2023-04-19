package manager

import (
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/node/persist"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
	"strconv"
	"strings"
	"sync"
)

var slotManagerOnce sync.Once
var slotManager *SlotManager

type SlotManager struct {
	slots         *Slots
	slotsReplicas cmap.ConcurrentMap[string, *SlotsReplica]
}

func GetSlotManagerInstance() *SlotManager {
	slotManagerOnce.Do(func() {
		slotManager = &SlotManager{
			NewSlots(),
			cmap.New[*SlotsReplica](),
		}
	})
	return slotManager
}

// Initializes the set of slots for which this node is responsible
func (s *SlotManager) initSlots(slots *queue.Array[string]) {
	if slots == nil || slots.IsEmpty() {
		for {
			slotsTemp, ok := GetServerMessageReceiverInstance().nodeSlotsQueue.Take()
			if !ok {
				continue
			}
			slots = &slotsTemp
			break
		}
	}
	for _, slotScope := range slots.Iter() {
		s.slots.init(slotScope)
	}
	persist.Persist(utils.ToJsonByte(slots), NodeSlotsFilename)
	log.Info.Println("The slot data of the node is initialized.")
}

// Initializes the collection of slot copies for which this node is responsible
func (s *SlotManager) initSlotsReplicas(slotScopes *queue.Array[string], isController bool) {
	if slotScopes == nil && !isController {
		for {
			slotScopesTemp, ok := GetServerMessageReceiverInstance().nodeSlotsReplicasQueue.Take()
			if !ok {
				continue
			}
			slotScopes = &slotScopesTemp
			break
		}
	} else if slotScopes == nil && isController {
		return
	}
	for _, slotScope := range slotScopes.Iter() {
		slotsReplica := NewSlotsReplica()
		slotsReplica.init(slotScope)
		s.slotsReplicas.Set(slotScope, slotsReplica)
	}
	persist.Persist(utils.ToJsonByte(slotScopes), NodeSlotsReplicasFilename)
	log.Info.Println("The copy data of the slot on the node is initialized.")
}

// Initializes the replica node id for the collection of slots for which this node is responsible
func (s *SlotManager) initReplicaNodeId(replicaNodeId int32) {
	if replicaNodeId == 0 {
		for {
			replicaNodeIdTemp, ok := GetServerMessageReceiverInstance().replicaNodeIdQueue.Take()
			if !ok {
				continue
			}
			replicaNodeId = replicaNodeIdTemp
			break
		}
	}
	s.slots.replicaNodeId = replicaNodeId
	log.Info.Println("The replica node id is initialized.")
}

func (s *SlotManager) getSlot(serviceName string) *Slot {
	slot, _ := s.slots.GetSlot(utils.RouteSlot(serviceName))
	return slot
}

// GetSlotReplica get slot replica
func (s *SlotManager) GetSlotReplica(serviceName string) *Slot {
	slotNo := utils.RouteSlot(serviceName)

	slotsReplica := &SlotsReplica{}
	for slotsReplicas := range s.slotsReplicas.IterBuffered() {
		slotScope := slotsReplicas.Key
		startSlot, _ := strconv.ParseInt(strings.Split(slotScope, ",")[0], 10, 32)
		endSlot, _ := strconv.ParseInt(strings.Split(slotScope, ",")[1], 10, 32)

		if slotNo >= int32(startSlot) && slotNo <= int32(endSlot) {
			slotsReplica, _ = s.slotsReplicas.Get(slotScope)
			break
		}
	}
	slot, _ := slotsReplica.slots.Get(slotNo)
	return slot
}

// Convert the copy to slots
func (s *SlotManager) changeReplicaToSlots(replicaSlotsList *queue.Array[string]) {
	// Take these slots out of the replica slots that the current node manages.
	// Move all the copy slots available to the official slot data set.
	for _, replicaSlots := range replicaSlotsList.Iter() {
		slotsReplica, ok := s.slotsReplicas.Get(replicaSlots)
		if !ok {
			continue
		}
		for iter := range slotsReplica.slots.IterBuffered() {
			s.slots.PutSlot(iter.Key, iter.Val)
		}
	}

	for _, replicaSlots := range replicaSlotsList.Iter() {
		s.slotsReplicas.Remove(replicaSlots)
	}
	log.Info.Printf("Replica slots [%s] all of them will be turned into regular employees.", replicaSlotsList)
}

// refresh slots replica
func (s *SlotManager) refreshReplicaSlots(replicaSlotsList *queue.Array[string]) {
	log.Info.Printf("The slot copy data is refreshed, old slots replica include [%s], new slots replica data is [%s]", s.slotsReplicas.Keys(), replicaSlotsList)

	for _, replicaSlots := range replicaSlotsList.Iter() {
		if !s.slotsReplicas.Has(replicaSlots) {
			slotsReplica := NewSlotsReplica()
			slotsReplica.init(replicaSlots)
			s.slotsReplicas.Set(replicaSlots, slotsReplica)
		}
	}
	persist.Persist(utils.ToJsonByte(replicaSlotsList), NodeSlotsReplicasFilename)
}

// The slot data is transferred
func (s *SlotManager) transferSlots(targetNodeId int32, slots string) {
	slotsSplit := strings.Split(slots, ",")
	startSlotNo, _ := strconv.ParseInt(slotsSplit[0], 10, 32)
	endSlotNo, _ := strconv.ParseInt(slotsSplit[1], 10, 32)
	for slotNo := int32(startSlotNo); slotNo <= int32(endSlotNo); slotNo++ {
		slot, ok := s.slots.GetSlot(slotNo)
		if !ok {
			continue
		}
		GetServerNetworkManagerInstance().sendMessage(targetNodeId,
			pkgrpc.MessageEntity_UPDATE_SLOTS, utils.Encode(&pkgrpc.UpdateSlotsRequest{
				SlotNo:   slotNo,
				SlotData: slot.ServiceRegistry.GetData(),
			}))
		s.slots.RemoteSlot(slotNo)
	}
}

func (s *SlotManager) refreshReplicaNodeId(newReplicaNodeId int32) {
	log.Info.Printf("The replica node id is refreshed, old node id: %d, new node id: %d", s.slots.replicaNodeId, newReplicaNodeId)
	s.slots.replicaNodeId = newReplicaNodeId
}
func (s *SlotManager) updateSlotData(slotNo int32, serviceInstances []*ServiceInstance) {
	slot, _ := s.slots.GetSlot(slotNo)
	slot.updateSlotData(serviceInstances)
}

type SlotsReplica struct {
	slots cmap.ConcurrentMap[int32, *Slot]
}

func NewSlotsReplica() *SlotsReplica {
	return &SlotsReplica{cmap.NewWithCustomShardingFunction[int32, *Slot](utils.Int32HashCode)}
}

// Initializes the collection of slots
func (s *SlotsReplica) init(slotScope string) {
	slotScopeSplit := strings.Split(slotScope, ",")

	startSlotNo, _ := strconv.ParseInt(slotScopeSplit[0], 10, 32)
	endSlotNo, _ := strconv.ParseInt(slotScopeSplit[1], 10, 32)

	serviceRegistry := NewServiceRegistry(false)

	for slotNo := int32(startSlotNo); slotNo <= int32(endSlotNo); slotNo++ {
		s.slots.Set(slotNo, NewSlot(slotNo, serviceRegistry))
	}
}

type Slots struct {
	slots         cmap.ConcurrentMap[int32, *Slot]
	replicaNodeId int32
}

func NewSlots() *Slots {
	return &Slots{slots: cmap.NewWithCustomShardingFunction[int32, *Slot](utils.Int32HashCode)}
}

func (s *Slots) init(slotScope string) {
	slotScopeSplit := strings.Split(slotScope, ",")

	startSlotNo, _ := strconv.ParseInt(slotScopeSplit[0], 10, 32)
	endSlotNo, _ := strconv.ParseInt(slotScopeSplit[1], 10, 32)

	serviceRegistry := NewServiceRegistry(false)

	for slotNo := int32(startSlotNo); slotNo <= int32(endSlotNo); slotNo++ {
		s.slots.Set(slotNo, NewSlot(slotNo, serviceRegistry))
	}
}

func (s *Slots) PutSlot(slotNo int32, slot *Slot) {
	s.slots.Set(slotNo, slot)
}

func (s *Slots) GetSlot(slotNo int32) (*Slot, bool) {
	return s.slots.Get(slotNo)
}

func (s *Slots) RemoteSlot(slotNo int32) {
	s.slots.Remove(slotNo)
}

type Slot struct {
	slotNo          int32
	ServiceRegistry *ServiceRegistry
}

func NewSlot(slotNo int32, serviceRegistry *ServiceRegistry) *Slot {
	return &Slot{slotNo, serviceRegistry}
}

func (s *Slot) isEmpty() bool {
	return s.ServiceRegistry.IsEmpty()
}

func (s *Slot) getSlotData() []byte {
	return s.ServiceRegistry.GetData()
}

func (s *Slot) updateSlotData(serviceInstances []*ServiceInstance) {
	s.ServiceRegistry.UpdateData(serviceInstances)
}
