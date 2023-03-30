package manager

import "sync"

const (
	commonNode = iota // 普通master 节点
	controller        // Controller 角色
	candidate         // Controller候选人角色
)
const (
	initializing = iota // 正在初始化
	running             // 正在运行中
	shutdown            // 已经关闭了
	fatal               // 系统崩溃了
)

var serverNodeRoleOnce sync.Once
var serverNodeRole *ServerNodeRole

// ServerNodeRole server node role
type ServerNodeRole struct {
	role int
}

func getServerNodeRoleInstance() *ServerNodeRole {
	serverNodeRoleOnce.Do(func() {
		serverNodeRole = &ServerNodeRole{
			role: commonNode,
		}
	})
	return serverNodeRole
}

func SetServerNodeRole(role int) {
	getServerNodeRoleInstance().role = role
}

func IsController() bool {
	return getServerNodeRoleInstance().role == controller
}

func isCandidate() bool {
	return getServerNodeRoleInstance().role == candidate
}

func isCommonNode() bool {
	return getServerNodeRoleInstance().role == commonNode
}

var controllerNodeOnce sync.Once
var controllerNode *ControllerNode

// ControllerNode controller where node
type ControllerNode struct {
	nodeId int
}

func getControllerNodeInstance() *ControllerNode {
	controllerNodeOnce.Do(func() {
		controllerNode = new(ControllerNode)
	})
	return controllerNode
}

func SetNodeId(nodeId int) {
	getControllerNodeInstance().nodeId = nodeId
}

func IsControllerNode(nodeId int) bool {
	return getControllerNodeInstance().nodeId == nodeId
}

var nodeStatusOnce sync.Once
var nodeStatus *NodeStatus

// NodeStatus node status
type NodeStatus struct {
	status int
	m      sync.Mutex
}

func getNodeStatusInstance() *NodeStatus {
	nodeStatusOnce.Do(func() {
		nodeStatus = new(NodeStatus)
	})
	return nodeStatus
}

func SetNodeStatus(status int) {
	nodeStatus := getNodeStatusInstance()
	nodeStatus.m.Lock()
	nodeStatus.m.Unlock()
	nodeStatus.status = status
}

func Running() {
	SetNodeStatus(running)
}

func IsRunning() bool {
	return getNodeStatusInstance().status == running
}

func IsShutdown() bool {
	return getNodeStatusInstance().status == shutdown
}

func Fatal() {
	SetNodeStatus(fatal)
}

func IsFatal() bool {
	return getNodeStatusInstance().status == fatal
}
