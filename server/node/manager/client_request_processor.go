package manager

import pkgrpc "github.com/Nextsummer/micro/pkg/grpc"

func processRequest(clientConnectionId string, request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {
	if pkgrpc.MessageEntity_CLIENT_REGISTER == request.GetType() {
		return register(request)
	}

	return nil
}

func register(request *pkgrpc.MessageEntity) *pkgrpc.MessageResponse {

	return &pkgrpc.MessageResponse{
		Success: true,
		Code:    pkgrpc.MessageResponse_REGISTER_SUCCESS,
		Data:    &pkgrpc.MessageEntity{RequestId: request.GetRequestId()},
	}
}
