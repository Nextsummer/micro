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
	"sync"
	"time"
)

type ServiceInstance struct {
	ServiceName         string `json:"serviceName"`
	ServiceInstanceIp   string `json:"serviceInstanceIp"`
	ServiceInstancePort int32  `json:"serviceInstancePort"`
	LatestHeartbeatTime int64  `json:"latestHeartbeatTime"`
}

func NewServiceInstance(serviceName, serviceInstanceIp string, serviceInstancePort int32) *ServiceInstance {
	return &ServiceInstance{
		ServiceName:         serviceName,
		ServiceInstanceIp:   serviceInstanceIp,
		ServiceInstancePort: serviceInstancePort,
	}
}

func NewRegisterToServiceInstance(request *pkgrpc.RegisterRequest) *ServiceInstance {
	return &ServiceInstance{
		ServiceName:         request.GetServiceName(),
		ServiceInstanceIp:   request.GetServiceInstanceIp(),
		ServiceInstancePort: request.GetServiceInstancePort(),
	}
}

func NewHeartbeatToServiceInstance(request *pkgrpc.HeartbeatRequest) *ServiceInstance {
	return &ServiceInstance{
		ServiceName:         request.GetServiceName(),
		ServiceInstanceIp:   request.GetServiceInstanceIp(),
		ServiceInstancePort: request.GetServiceInstancePort(),
	}
}

func (s ServiceInstance) String() string {
	return utils.ToJson(s)
}

func (s ServiceInstance) getServiceInstanceId() string {
	return fmt.Sprintf("%s_%s_%d", s.ServiceName, s.ServiceInstanceIp, s.ServiceInstancePort)
}

func (s ServiceInstance) GetAddress() string {
	return fmt.Sprintf("%s,%s,%d", s.ServiceName, s.ServiceInstanceIp, s.ServiceInstancePort)
}

// id of the generated service instance
func generatedServiceInstanceId(serviceName, serviceInstanceIp string, serviceInstancePort int32) string {
	return fmt.Sprintf("%s_%s_%d", serviceName, serviceInstanceIp, serviceInstancePort)
}

type ServiceRegistry struct {
	isReplica                  bool
	serviceRegistryData        cmap.ConcurrentMap[string, *queue.Array[*ServiceInstance]]
	serviceInstanceData        cmap.ConcurrentMap[string, *ServiceInstance]
	serviceChangedListenerData cmap.ConcurrentMap[string, *queue.Array[*ServiceChangedListener]]
	sync.RWMutex
}

func NewServiceRegistry(isReplica bool) *ServiceRegistry {
	s := &ServiceRegistry{
		isReplica:                  isReplica,
		serviceRegistryData:        cmap.New[*queue.Array[*ServiceInstance]](),
		serviceInstanceData:        cmap.New[*ServiceInstance](),
		serviceChangedListenerData: cmap.New[*queue.Array[*ServiceChangedListener]](),
	}
	go s.HeartbeatCheck()
	return s
}

func (s *ServiceRegistry) Register(serviceInstance *ServiceInstance) {
	serviceName := serviceInstance.ServiceName
	serviceInstances, ok := s.serviceRegistryData.Get(serviceName)
	if !ok {
		s.Lock()
		serviceInstances = queue.NewArray[*ServiceInstance]()
		s.serviceRegistryData.Set(serviceName, serviceInstances)
		s.Unlock()
	}
	for _, instanceStore := range serviceInstances.Iter() {
		if instanceStore.ServiceInstanceIp == serviceInstance.ServiceInstanceIp &&
			instanceStore.ServiceInstancePort == serviceInstance.ServiceInstancePort {
			s.Heartbeat(serviceInstance)
			return
		}
	}

	serviceInstances.Put(serviceInstance)
	s.serviceRegistryData.Set(serviceName, serviceInstances)
	s.serviceInstanceData.Set(serviceInstance.getServiceInstanceId(), serviceInstance)
	if !s.isReplica {
		serviceChangedListeners, ok := s.serviceChangedListenerData.Get(serviceName)
		if !ok {
			s.RWMutex.Lock()
			serviceChangedListeners = queue.NewArray[*ServiceChangedListener]()
			s.serviceChangedListenerData.Set(serviceName, serviceChangedListeners)
		}

		changedListenersIter := serviceChangedListeners.Iter()
		for i := range changedListenersIter {
			serviceRegistryInfo, ok := s.serviceRegistryData.Get(serviceName)
			if !ok {
				continue
			}
			changedListenersIter[i].onChange(serviceName, serviceRegistryInfo)
		}
	}
}

func (s *ServiceRegistry) Heartbeat(serviceInstance *ServiceInstance) {
	serviceInstanceId := serviceInstance.getServiceInstanceId()
	serviceInstanceTemp, ok := s.serviceInstanceData.Get(serviceInstanceId)
	if !ok {
		s.RWMutex.Lock()
		serviceInstanceTemp = serviceInstance
		serviceInstances, ok := s.serviceRegistryData.Get(serviceInstanceId)
		if !ok {
			serviceInstances = queue.NewArray[*ServiceInstance]()
			s.serviceRegistryData.Set(serviceInstance.ServiceName, serviceInstances)
		}
		serviceInstances.Put(serviceInstance)
		s.RWMutex.Unlock()
	}
	serviceInstanceTemp.LatestHeartbeatTime = time.Now().Unix()
	s.serviceInstanceData.Set(serviceInstanceId, serviceInstanceTemp)
	//log.Info.Printf("Received to %s heartbeat.", serviceInstance.getServiceInstanceId())
}

// Service subscription
func (s *ServiceRegistry) subscribe(clientConnectionId, serviceName string) *queue.Array[*ServiceInstance] {
	serviceChangedListeners, ok := s.serviceChangedListenerData.Get(serviceName)
	if !ok {
		s.RWMutex.Lock()
		serviceChangedListeners = queue.NewArray[*ServiceChangedListener]()
		s.serviceChangedListenerData.Set(serviceName, serviceChangedListeners)
		s.RWMutex.Unlock()
	}
	serviceChangedListeners.Put(&ServiceChangedListener{clientConnectionId})

	serviceRegistryInfo, ok := s.serviceRegistryData.Get(serviceName)
	if !ok {
		return queue.NewArray[*ServiceInstance]()
	}
	return serviceRegistryInfo

}

func (s *ServiceRegistry) UpdateData(serviceInstances []*ServiceInstance) {
	for _, serviceInstance := range serviceInstances {

		serviceInstances, ok := s.serviceRegistryData.Get(serviceInstance.ServiceName)
		if !ok {
			s.serviceRegistryData.Set(serviceInstance.getServiceInstanceId(), queue.NewArray[*ServiceInstance]())
		}
		serviceInstances.Put(serviceInstance)

		s.serviceInstanceData.Set(serviceInstance.getServiceInstanceId(), serviceInstance)
	}
}

func (s *ServiceRegistry) IsEmpty() bool {
	return s.serviceInstanceData.IsEmpty()
}

func (s *ServiceRegistry) GetData() []byte {
	var allServiceInstances []*ServiceInstance
	for serviceInstances := range s.serviceRegistryData.IterBuffered() {
		allServiceInstances = append(allServiceInstances, serviceInstances.Val.Iter()...)
	}
	return utils.ToJsonByte(allServiceInstances)
}

func (s *ServiceRegistry) GetRegisterService(serviceName string) map[string][]*ServiceInstance {
	registerInfo := make(map[string][]*ServiceInstance)
	if len(serviceName) == 0 {
		for data := range s.serviceRegistryData.IterBuffered() {
			registerInfo[data.Key] = data.Val.Iter()
		}
	} else {
		serviceInstances, ok := s.serviceRegistryData.Get(serviceName)
		if ok {
			registerInfo[serviceName] = serviceInstances.Iter()
		}
	}
	return registerInfo
}

func (s *ServiceRegistry) HeartbeatCheck() {
	configuration := config.GetConfigurationInstance()
	var removeServiceInstanceIds []string
	var changedServiceNames queue.Set[string]

	for IsRunning() {
		now := time.Now().Unix()

		serviceInstanceData := s.serviceInstanceData.Keys()
		for _, key := range serviceInstanceData {
			serviceInstance, _ := s.serviceInstanceData.Get(key)
			serviceName := serviceInstance.ServiceName
			if int32(now-serviceInstance.LatestHeartbeatTime) > configuration.HeartbeatTimeoutPeriod {
				serviceInstances, ok := s.serviceRegistryData.Get(serviceName)
				if ok && !serviceInstances.IsEmpty() {
					serviceInstances.Remove(serviceInstance)
					s.serviceRegistryData.Set(serviceName, serviceInstances)
					removeServiceInstanceIds = append(removeServiceInstanceIds, serviceInstance.getServiceInstanceId())
					changedServiceNames.Add(serviceInstance.getServiceInstanceId())
					log.Warn.Printf("No heartbeat is reported for the service instance within %d seconds, it has been removed: %v", configuration.HeartbeatTimeoutPeriod, utils.ToJson(serviceInstance))
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
					changedListenersIter[j].onChange(serviceName, serviceRegistryInfo)
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

func (s ServiceChangedListener) onChange(serviceName string, serviceInstances *queue.Array[*ServiceInstance]) {
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
