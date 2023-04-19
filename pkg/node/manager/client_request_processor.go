package manager

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/config"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	cmap "github.com/orcaman/concurrent-map/v2"
)

func processRequest(clientConnectionId string, request *pkgrpc.MessageEntity) (response *pkgrpc.MessageResponse) {
	messageType := request.GetType()
	if pkgrpc.MessageEntity_CLIENT_REGISTER == messageType {
		response = register(request)
	} else if pkgrpc.MessageEntity_CLIENT_FETCH_SLOTS_ALLOCATION == messageType {
		response = fetchSlotsAllocation(request)
	} else if pkgrpc.MessageEntity_CLIENT_FETCH_SERVER_ADDRESSES == messageType {
		response = fetchServerAddresses(request)
	} else if pkgrpc.MessageEntity_CLIENT_HEARTBEAT == messageType {
		response = heartbeat(request)
	} else if pkgrpc.MessageEntity_CLIENT_FETCH_SERVER_NODE_ID == messageType {
		response = fetchServerNodeId(request)
	} else if pkgrpc.MessageEntity_CLIENT_SUBSCRIBE == messageType {
		response = subscribe(clientConnectionId, request)
	} else {
		response = &pkgrpc.MessageResponse{Success: false, Message: "This message type has not yet been implemented, please reselect!"}
	}
	return
}

func register(request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {
	registerRequest := pkgrpc.RegisterRequest{}
	_ = utils.Decode(request.GetData(), &registerRequest)
	serviceName := registerRequest.GetServiceName()

	slotManager := GetSlotManagerInstance()
	slot := slotManager.getSlot(serviceName)
	slot.ServiceRegistry.Register(NewRegisterToServiceInstance(&registerRequest))
	log.Info.Printf("Complete the registration of the service instance [%s]", serviceName)

	// Construct the replica registration request and forward it to the specified node.
	GetServerNetworkManagerInstance().sendMessage(slotManager.slots.replicaNodeId, pkgrpc.MessageEntity_REPLICA_REGISTER, utils.Encode(&pkgrpc.RegisterRequest{
		ServiceName:         serviceName,
		ServiceInstanceIp:   registerRequest.GetServiceInstanceIp(),
		ServiceInstancePort: registerRequest.GetServiceInstancePort(),
	}))

	return &pkgrpc.MessageResponse{
		Success: true,
		Code:    pkgrpc.MessageResponse_REGISTER_SUCCESS,
		Result:  &pkgrpc.MessageEntity{RequestId: request.GetRequestId()},
	}
}

func subscribe(clientConnectionId string, request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {
	subscribeRequest := pkgrpc.SubscribeRequest{}
	_ = utils.Decode(request.GetData(), &subscribeRequest)
	serviceName := subscribeRequest.GetServiceName()

	slotManager := GetSlotManagerInstance()
	slot := slotManager.getSlot(serviceName)
	serviceInstances := slot.ServiceRegistry.subscribe(clientConnectionId, serviceName).Iter()

	var serviceInstanceAddresses []string
	for i := range serviceInstances {
		serviceInstance := serviceInstances[i]
		serviceInstanceAddresses = append(serviceInstanceAddresses, fmt.Sprintf("%s,%s,%d", serviceInstance.ServiceName, serviceInstance.ServiceInstanceIp, serviceInstance.ServiceInstancePort))
	}
	log.Info.Printf("Client [%s] subscribe service [%s] :%s", clientConnectionId, serviceName, serviceInstances)
	return &pkgrpc.MessageResponse{
		Success: true,
		Result: &pkgrpc.MessageEntity{
			RequestId: request.GetRequestId(),
			Data:      utils.Encode(&pkgrpc.SubscribeResponse{ServiceInstanceAddresses: serviceInstanceAddresses}),
		},
	}
}

func fetchSlotsAllocation(request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {
	slotsAllocation := cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](utils.Int32HashCode)

	if isCandidate() {
		slotsAllocation = GetControllerCandidateInstance().slotsAllocation
	} else if IsController() {
		slotsAllocation = getControllerInstance().slotsAllocation
	}

	return &pkgrpc.MessageResponse{
		Success: true,
		Result: &pkgrpc.MessageEntity{
			RequestId: request.GetRequestId(),
			Data:      utils.ToJsonByte(slotsAllocation),
		},
	}
}

func fetchServerAddresses(request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {
	return &pkgrpc.MessageResponse{
		Success: true,
		Result: &pkgrpc.MessageEntity{
			RequestId: request.GetRequestId(),
			Data: utils.Encode(&pkgrpc.FetchServerAddressesResponse{
				ServerAddresses: getServerAddresses(),
			}),
		},
	}
}

func fetchServerNodeId(request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {
	return &pkgrpc.MessageResponse{
		Success: true,
		Result: &pkgrpc.MessageEntity{
			RequestId: request.GetRequestId(),
			Data:      utils.Encode(&pkgrpc.FetchServerNodeIdResponse{ServerNodeId: config.GetConfigurationInstance().NodeId}),
		},
	}
}

func heartbeat(request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {
	heartbeatRequest := pkgrpc.HeartbeatRequest{}
	_ = utils.Decode(request.GetData(), &heartbeatRequest)
	serviceName := heartbeatRequest.GetServiceName()

	slotManager := GetSlotManagerInstance()
	slot := slotManager.getSlot(serviceName)
	slot.ServiceRegistry.Heartbeat(NewHeartbeatToServiceInstance(&heartbeatRequest))

	GetServerNetworkManagerInstance().sendMessage(slotManager.slots.replicaNodeId, pkgrpc.MessageEntity_REPLICA_HEARTBEAT, utils.Encode(&pkgrpc.RegisterRequest{
		ServiceName:         serviceName,
		ServiceInstanceIp:   heartbeatRequest.GetServiceInstanceIp(),
		ServiceInstancePort: heartbeatRequest.GetServiceInstancePort(),
	}))
	return &pkgrpc.MessageResponse{
		Success: true,
		Result:  &pkgrpc.MessageEntity{RequestId: request.GetRequestId()},
	}
}
