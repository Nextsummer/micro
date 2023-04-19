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
			slotReplica := slotManager.GetSlotReplica(serviceName)
			slotReplica.ServiceRegistry.Register(NewRegisterToServiceInstance(&registerRequest))
		}

		for {
			heartbeatRequest, ok := serverMessageReceiver.HeartbeatRequestQueue.Take()
			if !ok {
				break
			}
			serviceName := heartbeatRequest.GetServiceName()
			slotReplica := slotManager.GetSlotReplica(serviceName)
			slotReplica.ServiceRegistry.Heartbeat(NewHeartbeatToServiceInstance(&heartbeatRequest))
		}
	}

}
