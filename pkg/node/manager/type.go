package manager

import (
	"fmt"
	"hash/fnv"
	"sync"
)

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

func getServerNodeRole(role int) string {
	serverNodeRole := "Controller"
	switch role {
	case commonNode:
		serverNodeRole = "普通master"
	case candidate:
		serverNodeRole = "Controller候选节点"
	case controller:
		serverNodeRole = "Controller"
	}
	return serverNodeRole
}

var controllerNodeOnce sync.Once
var controllerNode *ControllerNode

// ControllerNode controller where node
type ControllerNode struct {
	nodeId int32
	sync.RWMutex
}

func GetControllerNodeInstance() *ControllerNode {
	controllerNodeOnce.Do(func() {
		controllerNode = &ControllerNode{}
	})
	return controllerNode
}

func (c *ControllerNode) SetControllerNodeId(nodeId int32) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	c.nodeId = nodeId
}

func (c *ControllerNode) IsControllerNode(nodeId int32) bool {
	c.RWMutex.RLock()
	defer c.RWMutex.RUnlock()
	return c.nodeId == nodeId
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

var Int32HashCode = func(key int32) uint32 {
	hash := fnv.New32()
	hash.Write([]byte(fmt.Sprintf("%v", key)))
	return hash.Sum32() >> 24
}
