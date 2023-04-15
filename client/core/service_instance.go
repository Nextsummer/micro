package core

import (
	"github.com/Nextsummer/micro/client/config"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/google/uuid"
	cmap "github.com/orcaman/concurrent-map/v2"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	RequestWaitSleepInterval = 10
	SelectorTimeout          = 5000
)

type ServiceInstance struct {
	serverConnectionManager       *ServerConnectionManager
	responses                     cmap.ConcurrentMap[string, *pkgrpc.MessageResponse]
	slotsAllocation               cmap.ConcurrentMap[int32, *queue.Array[string]]
	servers                       cmap.ConcurrentMap[int32, *config.Server]
	controllerCandidateConnection *ServerConnection
	controllerCandidate           config.Server
	serverConnection              *ServerConnection
	server                        *config.Server
	excludedRemoteAddress         string
	RunningState
}

func NewServiceInstance() *ServiceInstance {
	s := &ServiceInstance{
		serverConnectionManager: NewServerConnectionManager(),
		responses:               cmap.New[*pkgrpc.MessageResponse](),
		slotsAllocation:         cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](utils.Int32HashCode),
		servers:                 cmap.NewWithCustomShardingFunction[int32, *config.Server](utils.Int32HashCode),
	}
	go s.networkIO()
	return s
}

func (s *ServiceInstance) Init() {
	s.chooseControllerCandidate()
	s.controllerCandidateConnection = s.connectServer(s.controllerCandidate)
	id := s.fetchServerNodeId(s.controllerCandidate)
	s.controllerCandidate.SetId(id)
	s.controllerCandidateConnection.nodeId = id

	s.fetchSlotsAllocation(s.controllerCandidate)
	s.fetchServerAddresses(s.controllerCandidate)

	s.server = s.routeServer(config.GetConfigurationInstance().ServiceName)
	if id == s.server.GetId() {
		s.serverConnection = s.controllerCandidateConnection
	} else {
		s.serverConnection = s.connectServer(*s.server)
	}
}

// Pick a controller candidate at random
func (s *ServiceInstance) chooseControllerCandidate() {
	controllerCandidates := config.GetConfigurationInstance().ControllerCandidates
	var server config.Server

	for {
		server = controllerCandidates.RandomTake()
		if len(s.excludedRemoteAddress) == 0 ||
			!strings.EqualFold(server.GetRemoteSocketAddress(), s.excludedRemoteAddress) {
			break
		}
	}
	s.controllerCandidate = server
}

// Establish a persistent connection with the specified server
func (s *ServiceInstance) connectServer(server config.Server) *ServerConnection {
	address := server.GetRemoteSocketAddress()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Error.Fatalf("Client connection [%s] server failed, err: %v", address, err)
	}
	log.Info.Println("Connect to the server node: ", address)
	serverConnection := NewServerConnection(conn)
	s.serverConnectionManager.addServerConnection(serverConnection)
	return serverConnection
}

func (s *ServiceInstance) networkIO() {
	for {
		if s.serverConnection != nil {
			break
		}
	}

	go func() {
		for s.IsRunning() {
			responseBodyBytes, err := utils.ReadByte(s.serverConnection.conn)
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Error.Println("Client network io process decode server response failed, err: ", err)
				return
			}

			response := &pkgrpc.MessageResponse{}
			_ = utils.Decode(responseBodyBytes, response)
			log.Info.Println("Receive to server response: ", utils.ToJson(response))
			s.responses.Set(response.GetResult().GetRequestId(), response)
		}
	}()

	go func() {
		for s.IsRunning() {
			requestQueue, ok := GetServerMessageQueuesInstance().requestQueues.Get(s.serverConnection.connectionId)
			if !ok {
				continue
			}
			request, ok := requestQueue.Take()
			if !ok {
				continue
			}
			_, err := s.serverConnection.conn.Write(utils.Encode(&request))
			if err != nil {
				log.Error.Println("Client network io write request message to micro server failed, err: ", err)
				requestQueue.Put(request)
			}
		}
	}()
}

func (s *ServiceInstance) fetchServerNodeId(controllerCandidate config.Server) int32 {
	response := s.sendRequest(NewMessageEntity(pkgrpc.MessageEntity_CLIENT_FETCH_SERVER_NODE_ID, nil), controllerCandidate)
	if response.GetSuccess() {
		fetchServerNodeIdResponse := pkgrpc.FetchServerNodeIdResponse{}
		utils.Decode(response.GetResult().GetData(), &fetchServerNodeIdResponse)
		return fetchServerNodeIdResponse.GetServerNodeId()
	}
	log.Error.Fatalln("Fetch server node id failed, err: ", response.GetMessage())
	return 0
}

func (s *ServiceInstance) fetchSlotsAllocation(controllerCandidate config.Server) {
	response := s.sendRequest(NewMessageEntity(pkgrpc.MessageEntity_CLIENT_FETCH_SLOTS_ALLOCATION, nil), controllerCandidate)
	if !response.GetSuccess() {
		log.Error.Fatalln("Fetch slots allocation failed, err: ", response.GetMessage())
	}
	slotsAllocation := cmap.NewWithCustomShardingFunction[int32, *queue.Array[string]](utils.Int32HashCode)
	utils.BytesToJson(response.GetResult().GetData(), &slotsAllocation)
	s.slotsAllocation = slotsAllocation
	log.Info.Println("Pull the slot allocation data: ", response.GetSuccess())
}

func (s *ServiceInstance) fetchServerAddresses(controllerCandidate config.Server) {
	response := s.sendRequest(NewMessageEntity(pkgrpc.MessageEntity_CLIENT_FETCH_SERVER_ADDRESSES, nil), controllerCandidate)
	if !response.GetSuccess() {
		log.Error.Fatalln("Fetch server addresses failed, err: ", response.GetMessage())
	}
	serverAddresses := queue.NewArray[string]()
	utils.BytesToJson(response.GetResult().GetData(), &serverAddresses)

	if !serverAddresses.IsEmpty() {
		serverArray := serverAddresses.Iter()
		for i := range serverArray {
			serverAddressSplit := strings.Split(serverArray[i], ":")
			if len(serverAddressSplit) != 3 {
				continue
			}
			id, _ := strconv.ParseInt(serverAddressSplit[0], 10, 32)
			port, _ := strconv.ParseInt(serverAddressSplit[2], 10, 32)
			s.servers.Set(int32(id), config.NewServer(serverAddressSplit[1], int32(port)))
		}
		log.Info.Println("Pull the slot allocation data: ", utils.ToJson(serverAddresses))
	}
}

// Route the service instance to a server node
func (s *ServiceInstance) routeServer(serviceName string) *config.Server {
	slotNo := utils.RouteSlot(serviceName)
	serverId, _ := s.locateServerBySlot(slotNo)
	server, _ := s.servers.Get(serverId)
	log.Info.Println("The service instance is routed to the server node: ", utils.ToJson(server))
	return server
}

func (s *ServiceInstance) locateServerBySlot(slotNo int32) (int32, bool) {
	for slotsAllocation := range s.slotsAllocation.IterBuffered() {
		slots := slotsAllocation.Val.Iter()
		for i := range slots {
			slotsSplit := strings.Split(slots[i], ",")
			if len(slotsSplit) != 2 {
				continue
			}
			startSlot, _ := strconv.ParseInt(slotsSplit[0], 10, 32)
			endSlot, _ := strconv.ParseInt(slotsSplit[1], 10, 32)
			if slotNo >= int32(startSlot) && slotNo <= int32(endSlot) {
				return slotsAllocation.Key, true
			}

		}
	}
	return 0, false
}

func (s *ServiceInstance) Register() bool {
	configuration := config.GetConfigurationInstance()
	request := NewMessageEntity(pkgrpc.MessageEntity_CLIENT_REGISTER, utils.Encode(&pkgrpc.RegisterRequest{
		ServiceName:         configuration.ServiceName,
		ServiceInstanceIp:   configuration.ServiceInstanceIp,
		ServiceInstancePort: configuration.ServiceInstancePort,
	}))
	GetServerMessageQueuesInstance().putRequest(s.serverConnection.connectionId, request)
	log.Info.Println("Ready to send the service registration request, start waiting for the response result of the service registration.")
	for {
		if !s.responses.Has(request.GetRequestId()) {
			time.Sleep(RequestWaitSleepInterval)
		}
		break
	}
	log.Info.Println("The service registration has been successful.")
	s.responses.Remove(request.GetRequestId())
	go s.heartbeat()
	return true
}

func (s *ServiceInstance) heartbeat() {
	configuration := config.GetConfigurationInstance()

	for s.IsRunning() {
		request := NewMessageEntity(pkgrpc.MessageEntity_CLIENT_HEARTBEAT, utils.Encode(&pkgrpc.HeartbeatRequest{
			ServiceName:         configuration.ServiceName,
			ServiceInstanceIp:   configuration.ServiceInstanceIp,
			ServiceInstancePort: configuration.ServiceInstancePort,
		}))
		s.sendRequest(request, *s.server)
		log.Info.Println("Send heartbeat...")

		time.Sleep(time.Second * time.Duration(configuration.HeartbeatInterval))
	}
}

// Subscribe service subscribe
// 1. return a list of all instances of the specified service
// 2. If the specified service has a subsequent instance list change, actively push the change service data to the client
func (s *ServiceInstance) Subscribe(serviceName string) *queue.Array[ServiceInstanceAddress] {
	serviceRegistryCached := GetCachedServiceRegistryInstance()
	if serviceRegistryCached.isCached(serviceName) {
		serviceInstanceAddress, _ := serviceRegistryCached.serviceRegistry.Get(serviceName)
		return serviceInstanceAddress
	}
	server := *s.routeServer(serviceName)
	if !s.serverConnectionManager.hasConnected(server) {
		s.connectServer(server)
	}
	log.Info.Printf("The service is %s on the server [%v], ready to send a subscription request.", serviceName, server)
	request := NewMessageEntity(pkgrpc.MessageEntity_CLIENT_SUBSCRIBE, utils.Encode(&pkgrpc.SubscribeRequest{ServiceName: serviceName}))
	response := s.sendRequest(request, server)
	serviceInstanceAddress := queue.NewArray[ServiceInstanceAddress]()
	if !response.GetSuccess() {
		log.Error.Println("Client ")
		return serviceInstanceAddress
	}
	serviceInstanceAddressInfo := queue.NewArray[string]()
	utils.BytesToJson(response.GetResult().GetData(), &serviceInstanceAddressInfo)
	serviceInstanceAddressInfoIter := serviceInstanceAddressInfo.Iter()
	for i := range serviceInstanceAddressInfoIter {
		serviceInstanceAddressInfoSplit := strings.Split(serviceInstanceAddressInfoIter[i], ",")
		port, _ := strconv.ParseInt(serviceInstanceAddressInfoSplit[2], 10, 32)
		serviceInstanceAddress.Put(ServiceInstanceAddress{serviceInstanceAddressInfoSplit[0], serviceInstanceAddressInfoSplit[1], int32(port)})
	}
	serviceRegistryCached.cache(serviceName, serviceInstanceAddress)
	log.Info.Printf("Gets the latest instance address list [%s] of the service [%s]", utils.ToJson(serviceInstanceAddress), serviceName)
	return serviceInstanceAddress
}

// Send the request to the specified server
func (s *ServiceInstance) sendRequest(request *pkgrpc.MessageEntity, server config.Server) *pkgrpc.MessageResponse {
	serverConnection, _ := s.serverConnectionManager.serverConnections.Get(server.GetRemoteSocketAddress())
	GetServerMessageQueuesInstance().putRequest(serverConnection.connectionId, request)

	for {
		response, ok := s.responses.Get(request.GetRequestId())
		if !ok {
			time.Sleep(time.Millisecond * RequestWaitSleepInterval)
			continue
		}
		s.responses.Remove(request.GetRequestId())
		return response
	}
}

type RunningState struct {
	running bool
	sync.RWMutex
}

func NewRunningState() *RunningState {
	return &RunningState{running: true}
}
func (r *RunningState) Fatal() {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	r.running = false
}
func (r *RunningState) IsRunning() bool {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	return r.running
}

type ServiceInstanceAddress struct {
	id          string
	serviceName string
	port        int32
}

func NewMessageEntity(messageType pkgrpc.MessageEntity_MessageType, data []byte) *pkgrpc.MessageEntity {
	return &pkgrpc.MessageEntity{
		RequestId: uuid.New().String(),
		Type:      messageType,
		Data:      data,
	}
}
