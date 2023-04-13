package manager

import (
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/Nextsummer/micro/server/node/persist"
	"github.com/Nextsummer/micro/server/registry"
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

// GetSlotReplica get slot replica
func (s *SlotManager) GetSlotReplica(serviceName string) *Slot {
	slotNo := routeSlot(serviceName)

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

// Route the service to the slot
func routeSlot(serviceName string) int32 {
	hashCode := utils.StringHashCode(serviceName)
	slot := hashCode % SlotsCount
	if slot == 0 {
		slot++
	}
	return slot
}

func (s *SlotManager) refreshReplicaNodeId(newReplicaNodeId int32) {
	log.Info.Printf("The replica node id is refreshed, old node id: %d, new node id: %d", s.slots.replicaNodeId, newReplicaNodeId)
	s.slots.replicaNodeId = newReplicaNodeId
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
	endSlotNo, _ := strconv.ParseInt(slotScopeSplit[0], 10, 32)

	serviceRegistry := registry.NewServiceRegistry(false)

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
	endSlotNo, _ := strconv.ParseInt(slotScopeSplit[0], 10, 32)

	serviceRegistry := registry.NewServiceRegistry(false)

	for slotNo := int32(startSlotNo); slotNo <= int32(endSlotNo); slotNo++ {
		s.slots.Set(slotNo, NewSlot(slotNo, serviceRegistry))
	}
}

func (s *Slots) PutSlot(slotNo int32, slot *Slot) {
	s.slots.Set(slotNo, slot)
}

type Slot struct {
	slotNo          int32
	ServiceRegistry registry.ServiceRegistry
}

func NewSlot(slotNo int32, serviceRegistry registry.ServiceRegistry) *Slot {
	return &Slot{slotNo, serviceRegistry}
}

func (s *Slot) isEmpty() bool {
	return s.ServiceRegistry.IsEmpty()
}

func (s *Slot) getSlotData() []byte {
	return s.ServiceRegistry.GetData()
}

func (s *Slot) updateSlotData(serviceInstances []registry.ServiceInstance) {
	s.ServiceRegistry.UpdateData(serviceInstances)
}
