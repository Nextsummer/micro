package manager

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
			serviceInstance := NewServiceInstance(serviceName, registerRequest.GetServiceInstanceIp(), registerRequest.GetServiceInstancePort())
			slotReplica := slotManager.GetSlotReplica(serviceName)
			slotReplica.ServiceRegistry.Register(*serviceInstance)
		}

		for {
			heartbeatRequest, ok := serverMessageReceiver.HeartbeatRequestQueue.Take()
			if !ok {
				break
			}
			serviceName := heartbeatRequest.GetServiceName()
			serviceInstance := NewServiceInstance(serviceName, heartbeatRequest.GetServiceInstanceIp(), heartbeatRequest.GetServiceInstancePort())
			slotReplica := slotManager.GetSlotReplica(serviceName)
			slotReplica.ServiceRegistry.Heartbeat(*serviceInstance)
		}

	}

}
