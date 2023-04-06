package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	pkgrpc "github.com/Nextsummer/micro/pkg/grpc"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/queue"
	"github.com/Nextsummer/micro/pkg/server/config"
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/google/uuid"
	cmap "github.com/orcaman/concurrent-map/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	DefaultConnectRetries             = 3             // 默认的主动连接的重试次数
	ConnectTimeout                    = 5000          // 连接超时时间
	RetryConnectMasterNodeInterval    = 1 * 60 * 1000 // 重试连接master node的时间间隔
	CheckAllOtherNodesConnectInterval = 10 * 1000     // 检查跟其他所有节点的连接状态的时间间隔
	AllMasterNodeConnectCheckInterval = 100           // 等待所有master节点连接过来的检查间隔
	DefaultRetries                    = 3             // 默认的监听端口号的重试次数
)

func init() {

}

var serverNetworkManagerOnce sync.Once
var serverNetworkManager *ServerNetworkManager

type ServerNetworkManager struct {
	retryConnectMasterNodes []string                                                      // 等待重试发起连接的master节点列表
	remoteNodeClientStream  cmap.ConcurrentMap[int32, pkgrpc.Message_SendClient]          // 跟其他的远程master节点建立好的连接
	ioThreadRunningSignals  cmap.ConcurrentMap[int32, *IOThreadRunningSignal]             // 每个节点连接的读写IO线程是否运行的boolean变量
	sendQueues              cmap.ConcurrentMap[int32, *queue.Array[pkgrpc.MessageEntity]] // 发送请求队列
	receiveQueue            *queue.Array[pkgrpc.MessageEntity]                            // 接收请求队列
}

func GetServerNetworkManagerInstance() *ServerNetworkManager {
	serverNetworkManagerOnce.Do(func() {
		serverNetworkManager = &ServerNetworkManager{
			remoteNodeClientStream: cmap.NewWithCustomShardingFunction[int32, pkgrpc.Message_SendClient](Int32HashCode),
			ioThreadRunningSignals: cmap.NewWithCustomShardingFunction[int32, *IOThreadRunningSignal](Int32HashCode),
			sendQueues:             cmap.NewWithCustomShardingFunction[int32, *queue.Array[pkgrpc.MessageEntity]](Int32HashCode),
			receiveQueue:           queue.NewArray[pkgrpc.MessageEntity](),
		}
	})
	return serverNetworkManager
}

func (s *ServerNetworkManager) sendMessage(remoteNodeId int32, Type pkgrpc.MessageEntity_MessageType, data []byte) bool {
	sendQueue, ok := s.sendQueues.Get(remoteNodeId)
	if !ok {
		log.Error.Printf("The %d node has not been connected!", remoteNodeId)
		return false
	}
	sendQueue.Put(pkgrpc.MessageEntity{RequestId: uuid.New().String(), Type: Type, Data: data})
	return true
}

func ConnectAllControllerCandidates() bool {
	configuration := config.GetConfigurationInstance()
	servers := strings.Split(configuration.ControllerCandidateServers, ",")
	for _, endpoint := range servers {
		if !connectServerNode(endpoint) {
			continue
		}
	}
	return true
}

func ConnectBeforeControllerCandidateServers() bool {
	servers := config.GetBeforeControllerCandidateServers()

	for _, endpoint := range servers {
		if !connectServerNode(endpoint) {
			continue
		}
	}
	return true
}

// 等待跟所有的master节点建立连接
func waitAllServerNodeConnected() {
	log.Info.Println("等待跟所有的server节点都建立连接...")

	clusterNodeCount := config.GetConfigurationInstance().ClusterNodeCount

	allServerNodeConnected := false
	for !allServerNodeConnected {
		time.Sleep(AllMasterNodeConnectCheckInterval)

		if clusterNodeCount == int32(len(GetRemoteServerNodeManagerInstance().getRemoteServerNodes())+1) {
			allServerNodeConnected = true
		}
	}
	log.Info.Println("已经跟所有的server节点都建立连接...")
}

// 等待跟所有的controller候选节点完成连接
func waitAllControllerCandidatesConnected() {
	otherControllerCandidateServers := config.GetOtherControllerCandidateServers()
	log.Info.Printf("正在等待跟所有Controller候选节点建立连接: %v ...", otherControllerCandidateServers)

	for IsRunning() {
		allControllerCandidatesConnected := false
		if len(GetRemoteServerNodeManagerInstance().getRemoteServerNodes()) == len(otherControllerCandidateServers) {
			allControllerCandidatesConnected = true
		}
		if allControllerCandidatesConnected {
			log.Info.Println("已经跟所有Controller候选节点建立连接...")
			break
		}
		time.Sleep(CheckAllOtherNodesConnectInterval)
	}

}

func connectServerNode(endpoint string) bool {
	fatal := false
	conn, err := grpc.Dial(strings.Trim(endpoint, " "), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error.Fatalln("connect server node error, grpc dial error msg: ", err)
		return false
	}

	retries := 0

	for IsRunning() && retries <= DefaultConnectRetries {

		connBool, err := tryConnectServerNode(conn)
		if !connBool {
			return true
		} else {
			log.Error.Printf("与server节点(%v)建立连接的过程中发生异常! 异常为: %v", endpoint, err)
			fatal = true
			retries++
			if retries <= DefaultConnectRetries {
				log.Warn.Printf("这是第%v次重试连接server节点(%v)...", retries, endpoint)
			}
		}
	}
	if fatal {
		Fatal()
	}
	return false
}

func tryConnectServerNode(conn *grpc.ClientConn) (bool, error) {
	defer func() {
		if err := recover(); err != nil {
			log.Error.Println("try connect server node error, error msg: ", err)
		}
	}()
	messageClient := pkgrpc.NewMessageClient(conn)
	remoteServerNode, ok := exchangeSelfInformation(messageClient, func() { conn.Close() })
	if !ok || len(remoteServerNode.GetIp()) <= 0 {
		return true, nil
	}
	messageClientStream, err := messageClient.Send(context.TODO())
	if err != nil {
		return true, err
	}

	startServerIOThreads(remoteServerNode.GetNodeId(), messageClientStream)
	addRemoteNodeClientStream(remoteServerNode.GetNodeId(), messageClientStream)
	GetRemoteServerNodeManagerInstance().addRemoteServerNode(remoteServerNode)

	remoteServerNodeInfoJson, _ := json.Marshal(remoteServerNode)
	log.Info.Printf("完成与远程server节点的连接：%v ......", string(remoteServerNodeInfoJson))

	if IsController() {
		//todo auto rebalance
	}
	return false, nil
}

func exchangeSelfInformation(client pkgrpc.MessageClient, close func()) (pkgrpc.RemoteServerNode, bool) {
	configuration := config.GetConfigurationInstance()

	response, err := client.RemoteNodeInfo(context.TODO(), &pkgrpc.RemoteServerNode{
		NodeId:                configuration.NodeId,
		IsControllerCandidate: configuration.IsControllerCandidate,
		Ip:                    configuration.NodeIp,
		InternPort:            configuration.NodeInternTcpPort,
		ClientPort:            configuration.NodeClientTcpPort,
		IsController:          IsController(),
	})
	if status.Code(err) == codes.Unavailable {
		log.Info.Printf("远程server[%v]节点网络尚未初始化完毕，请稍等...", configuration.NodeIp)
		close()
		return pkgrpc.RemoteServerNode{}, false
	}
	if err != nil {
		log.Warn.Println("发送本节点信息给刚建立连接的server节点，出现通信异常！异常为：", err)
		close()
		return pkgrpc.RemoteServerNode{}, false
	}
	return *response.GetData(), response.GetSuccess()
}

func startServerIOThreads(remoteNodeId int32, messageClientStream pkgrpc.Message_SendClient) {
	sendQueue := queue.NewArray[pkgrpc.MessageEntity]()
	manager := GetServerNetworkManagerInstance()
	manager.sendQueues.Set(remoteNodeId, sendQueue)

	ioThreadRunning := &IOThreadRunningSignal{isRunning: true}
	manager.ioThreadRunningSignals.Set(remoteNodeId, ioThreadRunning)

	go startWriteIOThead(remoteNodeId, sendQueue, ioThreadRunning, messageClientStream)
	go startReadIOThead(remoteNodeId, manager.receiveQueue, ioThreadRunning, messageClientStream)
}

func startReadIOThead(remoteNodeId int32, receiveQueue *queue.Array[pkgrpc.MessageEntity],
	ioThreadRunning *IOThreadRunningSignal, stream pkgrpc.Message_SendClient) {
	for IsRunning() && ioThreadRunning.IsRunning() {
		response, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Error.Printf("从节点[%v]读取数据时，发生未知IO异常: %v", remoteNodeId, err)
			Fatal()
			return
		}
		if !response.GetSuccess() || response.GetData() == nil {
			log.Error.Printf("从节点[%v]读取数据失败, error message: %v", remoteNodeId, response.GetSuccess())
		}
		receiveQueue.Put(*response.GetData())
	}
}

func startWriteIOThead(remoteNodeId int32,
	sendQueue *queue.Array[pkgrpc.MessageEntity],
	ioThreadRunning *IOThreadRunningSignal,
	stream pkgrpc.Message_SendClient) {

	for IsRunning() && ioThreadRunning.IsRunning() {
		message, ok := sendQueue.Take()
		if !ok {
			continue
		}
		err := stream.Send(&message)
		if err != nil {
			Fatal()
			log.Error.Println("send message to remote node error: ", err)
		}
	}

	log.Info.Printf("跟节点[%v]的网络连接的写IO协程，即将终止运行...", remoteNodeId)
	if IsFatal() {
		log.Info.Printf("跟节点[%v]的网络连接的写IO线程，遇到不可逆转的重大事故，系统即将崩溃...", remoteNodeId)
	}
}

func StartServerConnectionListener() {
	s := &ServerConnectionListener{
		retries: 0,
	}
	go func() {
		s.run()
	}()
}

type ServerConnectionListener struct {
	retries int
}

func (s *ServerConnectionListener) run() {
	execute := IsRunning() && s.retries <= DefaultRetries

	fatal := false
	for execute {
		err := s.tryListenerConnect()
		if err != nil {
			fatal = true
			s.retries++
			log.Error.Fatalln("server listener connect error, error msg: ", err)
		}
	}
	if fatal {
		Fatal()
		log.Error.Fatalln("无法正常监听其他server节点的连接请求！")
	}
}

func (*ServerConnectionListener) tryListenerConnect() error {
	defer func() error {
		if err := recover(); err != nil {
			return errors.New(fmt.Sprintf("%v", err))
		}
		return nil
	}()
	port := config.GetConfigurationInstance().NodeInternTcpPort

	listen, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		panic("listen failed, err: " + err.Error())
	}

	s := grpc.NewServer()
	pkgrpc.RegisterMessageServer(s, &server{})

	log.Info.Printf("server连接请求线程，已经绑定端口号:%v，等待监听连接请求......", port)
	for IsRunning() {
		err := s.Serve(listen)
		if err != nil {
			panic("grpc accept failed, err: " + err.Error())
		}
	}
	return nil
}

func addRemoteNodeClientStream(remoteNodeId int32, client pkgrpc.Message_SendClient) {
	GetServerNetworkManagerInstance().remoteNodeClientStream.Set(remoteNodeId, client)
}

func removeRemoteNodeClientStream(remoteNodeId int32) {
	GetServerNetworkManagerInstance().remoteNodeClientStream.Remove(remoteNodeId)
}

func getRemoteNodeClientStream(remoteNodeId int32) pkgrpc.Message_SendClient {
	client, ok := GetServerNetworkManagerInstance().remoteNodeClientStream.Get(remoteNodeId)
	if !ok {
		log.Error.Printf("该远程server节点客户端数据流尚未注册...")
	}
	return client
}

type server struct {
	pkgrpc.UnimplementedMessageServer
	sync.Mutex
}

func (s *server) mustEmbedUnimplementedMessageServer() {
	// ignore
}

func (s *server) RemoteNodeInfo(ctx context.Context, remoteNodeInfo *pkgrpc.RemoteServerNode) (*pkgrpc.RemoteServerNodeResponse, error) {
	if !IsRunning() {
		log.Error.Println("该server节点服务异常...")
		return nil, errors.New("该server节点服务异常")
	}

	if remoteNodeInfo == nil {
		return nil, nil
	}
	log.Info.Printf("收到remoteNodeInfo: %v", remoteNodeInfo)

	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", remoteNodeInfo.GetIp(), remoteNodeInfo.GetInternPort()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Did not connect, err: " + err.Error())
	}
	messageClientStream, err := pkgrpc.NewMessageClient(conn).Send(context.TODO())
	if err != nil {
		panic("new message client stream err: " + err.Error())
	}

	startServerIOThreads(remoteNodeInfo.GetNodeId(), messageClientStream)
	addRemoteNodeClientStream(remoteNodeInfo.GetNodeId(), messageClientStream)
	GetRemoteServerNodeManagerInstance().addRemoteServerNode(*remoteNodeInfo)

	if IsController() {
		// todo auto rebalance manager
	}
	log.Info.Printf("连接监听线程已经跟远程server[%v]节点建立连接，IO协程全部启动...", remoteNodeInfo.GetNodeId())

	configuration := config.GetConfigurationInstance()
	return &pkgrpc.RemoteServerNodeResponse{
		Success: true,
		Message: "success",
		Data: &pkgrpc.RemoteServerNode{
			NodeId:                configuration.NodeId,
			IsControllerCandidate: configuration.IsControllerCandidate,
			Ip:                    configuration.NodeIp,
			InternPort:            configuration.NodeInternTcpPort,
			ClientPort:            configuration.NodeClientTcpPort,
			IsController:          IsController(),
		},
	}, nil
}

func (s *server) Send(stream pkgrpc.Message_SendServer) error {

	for IsRunning() {
		message, err := stream.Recv()
		if err == io.EOF {
			log.Info.Println("echo last received message")
			// ack
			return stream.Send(&pkgrpc.MessageResponse{
				Success: true,
				Message: "success",
			})
		}
		if err != nil {
			log.Error.Fatalln("received message error, error msg: {}", err)
			return err
		}

		log.Info.Println("request message received: ", utils.ToJson(*message))
		GetServerNetworkManagerInstance().receiveQueue.Put(*message)
		sendQueues := GetServerNetworkManagerInstance().sendQueues
		for item := range sendQueues.IterBuffered() {
			message, ok := item.Val.Take()
			if !ok {
				continue
			}
			remoteNodeId := item.Key
			err := getRemoteNodeClientStream(remoteNodeId).Send(&message)
			if err != nil {
				log.Error.Fatalln("远程server节点消息发送失败，异常为：", err)
				//发送失败认为远程server节点下线或网络故障，将数据保存到 manager.sendQueues 中待发送
				sendQueue, ok := sendQueues.Get(remoteNodeId)
				if !ok {
					sendQueues.Set(remoteNodeId, queue.NewArray[pkgrpc.MessageEntity]())
				}
				sendQueue.Put(message)
			}
		}
	}

	return nil
}

func (s *server) bufferExchange(current map[int32]*queue.Array[pkgrpc.MessageEntity]) map[int32][]pkgrpc.MessageEntity {
	s.Lock()
	defer s.Unlock()
	exchange := make(map[int32][]pkgrpc.MessageEntity, len(current))
	for key, _ := range current {
		exchange[key] = current[key].ClearAndIter()
	}
	return exchange
}

var remoteServerNodeManagerOnce sync.Once
var remoteServerNodeManager *RemoteServerNodeManager

type RemoteServerNodeManager struct {
	remoteServerNodes cmap.ConcurrentMap[int32, pkgrpc.RemoteServerNode]
}

func GetRemoteServerNodeManagerInstance() *RemoteServerNodeManager {
	remoteServerNodeManagerOnce.Do(func() {
		remoteServerNodeManager = &RemoteServerNodeManager{
			remoteServerNodes: cmap.NewWithCustomShardingFunction[int32, pkgrpc.RemoteServerNode](Int32HashCode),
		}
	})
	return remoteServerNodeManager
}

func (r *RemoteServerNodeManager) addRemoteServerNode(remoteServerNode pkgrpc.RemoteServerNode) {
	r.remoteServerNodes.Set(remoteServerNode.GetNodeId(), remoteServerNode)
}

// Get all the remote master nodes
func (r *RemoteServerNodeManager) getRemoteServerNodes() []pkgrpc.RemoteServerNode {
	var remoteServerNodes []pkgrpc.RemoteServerNode
	for item := range r.remoteServerNodes.IterBuffered() {
		remoteServerNodes = append(remoteServerNodes, item.Val)
	}
	return remoteServerNodes
}

// Gets the other controller candidate nodes
func (r *RemoteServerNodeManager) getOtherControllerCandidates() []pkgrpc.RemoteServerNode {
	var remoteServerNodes []pkgrpc.RemoteServerNode
	for item := range r.remoteServerNodes.IterBuffered() {
		if item.Val.IsControllerCandidate {
			remoteServerNodes = append(remoteServerNodes, item.Val)
		}
	}
	return remoteServerNodes
}

func (r *RemoteServerNodeManager) removeServerNode(remoteNodeId int32) {
	r.remoteServerNodes.Remove(remoteNodeId)
}

func (r *RemoteServerNodeManager) getRemoteServerNode(remoteNodeId int32) (pkgrpc.RemoteServerNode, bool) {
	return r.remoteServerNodes.Get(remoteNodeId)
}

func (r *RemoteServerNodeManager) hasController() bool {
	for item := range r.remoteServerNodes.IterBuffered() {
		if item.Val.IsController {
			return true
		}
	}
	return false
}

func (r *RemoteServerNodeManager) getController() pkgrpc.RemoteServerNode {
	for item := range r.remoteServerNodes.IterBuffered() {
		if item.Val.IsController {
			return item.Val
		}
	}
	return pkgrpc.RemoteServerNode{}
}

// IOThreadRunningSignal io thread running signal
type IOThreadRunningSignal struct {
	isRunning bool
	m         sync.RWMutex
}

func (i *IOThreadRunningSignal) SetIsRunning(isRunning bool) {
	i.m.Lock()
	defer i.m.Unlock()
	i.isRunning = isRunning
}

func (i *IOThreadRunningSignal) IsRunning() bool {
	i.m.RLock()
	defer i.m.RUnlock()
	return i.isRunning
}
