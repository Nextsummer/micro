package manager

import (
	"github.com/Nextsummer/micro/server/registry"
)

// ReplicationManager Replica copy component
func ReplicationManager() {
	serverMessageReceiver := GetServerMessageReceiverInstance()
	slotManager := GetSlotManagerInstance()

	for IsRunning() {
		for {
			registerRequest, ok := serverMessageReceiver.RegisterRequestQueue.Take()
			if !ok {
				break
			}
			serviceName := registerRequest.GetServiceName()
			serviceInstance := registry.NewServiceInstance(serviceName, registerRequest.GetServiceInstanceIp(), registerRequest.GetServiceInstancePort())
			slotReplica := slotManager.GetSlotReplica(serviceName)
			slotReplica.ServiceRegistry.Register(*serviceInstance)
		}

		for {
			heartbeatRequest, ok := serverMessageReceiver.HeartbeatRequestQueue.Take()
			if !ok {
				break
			}
			serviceName := heartbeatRequest.GetServiceName()
			serviceInstance := registry.NewServiceInstance(serviceName, heartbeatRequest.GetServiceInstanceIp(), heartbeatRequest.GetServiceInstancePort())
			slotReplica := slotManager.GetSlotReplica(serviceName)
			slotReplica.ServiceRegistry.Heartbeat(*serviceInstance)
		}

	}

}
