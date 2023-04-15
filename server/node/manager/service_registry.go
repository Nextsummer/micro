package manager

import (
	"fmt"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/Nextsummer/micro/server/config"
	"github.com/google/uuid"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
	"time"
)

type ServiceInstance struct {
	serviceName         string
	serviceInstanceIp   string
	serviceInstancePort int32
	latestHeartbeatTime int64
}

func NewServiceInstance(serviceName, serviceInstanceIp string, serviceInstancePort int32) *ServiceInstance {
	return &ServiceInstance{
		serviceName:         serviceName,
		serviceInstanceIp:   serviceInstanceIp,
		serviceInstancePort: serviceInstancePort,
	}
}

func (s ServiceInstance) String() string {
	return utils.ToJson(s)
}

func (s ServiceInstance) getServiceInstanceId() string {
	return fmt.Sprintf("%s_%s_%d", s.serviceName, s.serviceInstanceIp, s.serviceInstancePort)
}

func (s ServiceInstance) GetAddress() string {
	return fmt.Sprintf("%s,%s,%d", s.serviceName, s.serviceInstanceIp, s.serviceInstancePort)
}

// id of the generated service instance
func generatedServiceInstanceId(serviceName, serviceInstanceIp string, serviceInstancePort int32) string {
	return fmt.Sprintf("%s_%s_%d", serviceName, serviceInstanceIp, serviceInstancePort)
}

type ServiceRegistry struct {
	isReplica                  bool
	serviceRegistryData        cmap.ConcurrentMap[string, *queue.Array[ServiceInstance]]
	serviceInstanceData        cmap.ConcurrentMap[string, ServiceInstance]
	serviceChangedListenerData cmap.ConcurrentMap[string, *queue.Array[ServiceChangedListener]]
	sync.RWMutex
}

func NewServiceRegistry(isReplica bool) *ServiceRegistry {
	s := &ServiceRegistry{
		isReplica:           isReplica,
		serviceRegistryData: cmap.New[*queue.Array[ServiceInstance]](),
		serviceInstanceData: cmap.New[ServiceInstance](),
	}
	go s.HeartbeatCheck()
	return s
}

func (s *ServiceRegistry) Register(serviceInstance ServiceInstance) {
	serviceName := serviceInstance.serviceName
	serviceInstances, ok := s.serviceRegistryData.Get(serviceName)
	if !ok {
		s.Lock()
		serviceInstances = queue.NewArray[ServiceInstance]()
		s.serviceRegistryData.Set(serviceName, serviceInstances)
		s.Unlock()
	}
	serviceInstances.Put(serviceInstance)

	s.serviceInstanceData.Set(serviceInstance.getServiceInstanceId(), serviceInstance)

	if !s.isReplica {
		serviceChangedListeners, ok := s.serviceChangedListenerData.Get(serviceName)
		if !ok {
			s.RWMutex.Lock()
			serviceChangedListeners = queue.NewArray[ServiceChangedListener]()
			s.serviceChangedListenerData.Set(serviceName, serviceChangedListeners)
		}

		changedListenersIter := serviceChangedListeners.Iter()
		for i := range changedListenersIter {
			serviceRegistryInfo, ok := s.serviceRegistryData.Get(serviceName)
			if !ok {
				continue
			}
			changedListenersIter[i].onChange(serviceName, *serviceRegistryInfo)
		}
	}
}

func (s *ServiceRegistry) Heartbeat(serviceInstance ServiceInstance) {
	serviceInstanceId := serviceInstance.getServiceInstanceId()
	serviceInstance, ok := s.serviceInstanceData.Get(serviceInstanceId)
	if !ok {
		s.RWMutex.Lock()
		serviceInstance, ok = s.serviceInstanceData.Get(serviceInstanceId)
		if !ok {
			s.serviceInstanceData.Set(serviceInstanceId, serviceInstance)
			serviceInstances, ok := s.serviceRegistryData.Get(serviceInstanceId)
			if !ok {
				s.serviceRegistryData.Set(serviceInstanceId, queue.NewArray[ServiceInstance]())
			}
			serviceInstances.Put(serviceInstance)
		}
		s.RWMutex.Unlock()
	}
	serviceInstance.latestHeartbeatTime = time.Now().Unix()
	log.Info.Printf("Received to %d heartbeat.", serviceInstance.getServiceInstanceId())
}

// Service subscription
func (s *ServiceRegistry) subscribe(clientConnectionId, serviceName string) *queue.Array[ServiceInstance] {
	serviceChangedListeners, ok := s.serviceChangedListenerData.Get(serviceName)
	if !ok {
		s.RWMutex.Lock()
		s.serviceChangedListenerData.Set(serviceName, queue.NewArray[ServiceChangedListener]())
		s.RWMutex.Unlock()
	}
	serviceChangedListeners.Put(ServiceChangedListener{clientConnectionId})

	serviceRegistryInfo, ok := s.serviceRegistryData.Get(serviceName)
	if !ok {
		return queue.NewArray[ServiceInstance]()
	}
	return serviceRegistryInfo

}

func (s *ServiceRegistry) UpdateData(serviceInstances []ServiceInstance) {
	for _, serviceInstance := range serviceInstances {
		serviceName := serviceInstance.serviceName

		serviceInstances, ok := s.serviceRegistryData.Get(serviceName)
		if !ok {
			s.serviceRegistryData.Set(serviceName, queue.NewArray[ServiceInstance]())
		}
		serviceInstances.Put(serviceInstance)

		s.serviceInstanceData.Set(serviceInstance.getServiceInstanceId(), serviceInstance)
	}
}

func (s *ServiceRegistry) IsEmpty() bool {
	return s.serviceInstanceData.IsEmpty()
}

func (s *ServiceRegistry) GetData() []byte {
	var allServiceInstances []ServiceInstance
	for serviceInstances := range s.serviceRegistryData.IterBuffered() {
		allServiceInstances = append(allServiceInstances, serviceInstances.Val.Iter()...)
	}
	return utils.ToJsonByte(allServiceInstances)
}

func (s *ServiceRegistry) HeartbeatCheck() {
	configuration := config.GetConfigurationInstance()
	var removeServiceInstanceIds []string
	var changedServiceNames queue.Set[string]

	for IsRunning() {
		now := time.Now().Unix()

		for iter := range s.serviceInstanceData.IterBuffered() {
			serviceInstance := iter.Val
			if int32(now-serviceInstance.latestHeartbeatTime) > configuration.HeartbeatTimeoutPeriod*1000 {
				serviceInstances, ok := s.serviceRegistryData.Get(serviceInstance.serviceName)
				if ok {
					serviceInstances.Remove(serviceInstance)
					removeServiceInstanceIds = append(removeServiceInstanceIds, serviceInstance.serviceName)
					changedServiceNames.Add(serviceInstance.getServiceInstanceId())
					log.Warn.Printf("No heartbeat is reported for the service instance within %s seconds, it has been removed: %s", configuration.HeartbeatTimeoutPeriod, serviceInstance)
				}
			}
		}
		for i := range removeServiceInstanceIds {
			s.serviceInstanceData.Remove(removeServiceInstanceIds[i])
		}
		if !s.isReplica {
			changedServiceNamesIter := changedServiceNames.Iter()
			for i := range changedServiceNamesIter {
				serviceName := changedServiceNamesIter[i]
				serviceChangedListeners, ok := s.serviceChangedListenerData.Get(serviceName)
				if !ok {
					continue
				}
				changedListenersIter := serviceChangedListeners.Iter()
				for j := range changedListenersIter {
					serviceRegistryInfo, ok := s.serviceRegistryData.Get(serviceName)
					if !ok {
						continue
					}
					changedListenersIter[j].onChange(serviceName, *serviceRegistryInfo)
				}
			}
		}

		removeServiceInstanceIds = append(removeServiceInstanceIds, removeServiceInstanceIds[0:0]...)
		changedServiceNames.Clear()

		time.Sleep(time.Second * time.Duration(configuration.HeartbeatCheckInterval))
	}
}

type ServiceChangedListener struct {
	clientConnectionId string
}

func (s ServiceChangedListener) onChange(serviceName string, serviceInstances queue.Array[ServiceInstance]) {
	var serviceInstanceAddress []string
	serviceIter := serviceInstances.Iter()
	for i := range serviceIter {
		serviceInstanceAddress = append(serviceInstanceAddress, serviceIter[i].GetAddress())
	}

	GetClientMessageQueuesInstance().putMessage(s.clientConnectionId, &pkgrpc.MessageResponse{
		Success: true,
		Result: &pkgrpc.MessageEntity{
			RequestId: uuid.New().String(),
			Type:      pkgrpc.MessageEntity_CLIENT_SERVICE_CHANGED,
			Data: utils.Encode(&pkgrpc.ServiceChangedRequest{
				ServiceName:              serviceName,
				ServiceInstanceAddresses: serviceInstanceAddress,
			}),
		},
	})
}
