package registry

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
)

type ServiceInstance struct {
	serviceName         string
	serviceInstanceIp   string
	serviceInstancePort int32
	latestHeartbeatTime int64
}

func (s *ServiceInstance) String() string {
	return utils.ToJson(s)
}

func (s *ServiceInstance) getServiceInstanceId() string {
	return fmt.Sprintf("%s_%s_%d", s.serviceName, s.serviceInstanceIp, s.serviceInstancePort)
}

// id of the generated service instance
func generatedServiceInstanceId(serviceName, serviceInstanceIp string, serviceInstancePort int32) string {
	return fmt.Sprintf("%s_%s_%d", serviceName, serviceInstanceIp, serviceInstancePort)
}

type ServiceRegistry struct {
	isReplica           bool
	serviceRegistryData cmap.ConcurrentMap[string, *queue.Array[ServiceInstance]]
	serviceInstanceData cmap.ConcurrentMap[string, ServiceInstance]
	sync.RWMutex
}

func NewServiceRegistry(isReplica bool) ServiceRegistry {
	return ServiceRegistry{
		isReplica:           isReplica,
		serviceRegistryData: cmap.New[*queue.Array[ServiceInstance]](),
		serviceInstanceData: cmap.New[ServiceInstance](),
	}
}

func (s *ServiceRegistry) Register(serviceInstance ServiceInstance) {
	serviceInstances, ok := s.serviceRegistryData.Get(serviceInstance.serviceName)
	if !ok {
		s.Lock()
		serviceInstances = queue.NewArray[ServiceInstance]()
		s.serviceRegistryData.Set(serviceInstance.serviceName, serviceInstances)
		s.Unlock()
	}
	serviceInstances.Put(serviceInstance)

	s.serviceInstanceData.Set(serviceInstance.getServiceInstanceId(), serviceInstance)

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
