package config

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/utils"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	NodeId                        = "node.id"
	NodeIp                        = "node.ip"
	NodeInternTcpPort             = "node.intern.tcp.port"
	NodeClientHttpPort            = "node.client.http.port"
	NodeClientTcpPort             = "node.client.tcp.port"
	IsControllerCandidate         = "is.controller.candidate"
	ClusterNodeCount              = "cluster.node.count"
	DataDir                       = "data.dir"
	ControllerCandidateServers    = "controller.candidate.servers"
	HeartbeatCheckInterval        = "heartbeat.check.interval"
	HeartbeatTimeoutPeriod        = "heartbeat.timeout.period"
	DefaultHeartbeatCheckInterval = 3
	DefaultHeartbeatTimeoutPeriod = 5
)

type DeploymentType string

const (
	Single  DeploymentType = "single"
	Cluster DeploymentType = "cluster"
)

var configurationOnce sync.Once
var singleton *Configuration

type Configuration struct {
	NodeId                     int32  // 节点id
	NodeIp                     string // 节点ip
	NodeInternTcpPort          int32  //节点内部通信的TCP端口号
	nodeClientHttpPort         int32  //跟客户端通信的HTTP端口号
	NodeClientTcpPort          int32  //跟客户端通信的TCP端口号
	IsControllerCandidate      bool   //是否为controller候选节点
	DataDir                    string //数据存储目录
	ClusterNodeCount           int32  //集群节点总数量
	ControllerCandidateServers string //controller候选节点的机器列表
	HeartbeatCheckInterval     int32  //心跳检查时间间隔
	HeartbeatTimeoutPeriod     int32  //心跳超时时间
	DeploymentType
}

func GetConfigurationInstance() *Configuration {
	configurationOnce.Do(func() {
		singleton = &Configuration{}
	})
	return singleton
}

func Parse(configPath string) {
	configMap := utils.LoadConfigurationFile(configPath)

	configuration := GetConfigurationInstance()
	configuration.NodeId = validateNodeId(configMap[NodeId])
	configuration.NodeIp = validateNodeIp(configMap[NodeIp])
	configuration.NodeInternTcpPort = validateNodeInternTcpPort(configMap[NodeInternTcpPort])
	configuration.nodeClientHttpPort = validateNodeClientHttpPort(configMap[NodeClientHttpPort])
	configuration.NodeClientTcpPort = validateNodeClientTcpPort(configMap[NodeClientTcpPort])
	configuration.IsControllerCandidate = validateIsControllerCandidate(configMap[IsControllerCandidate])
	configuration.DataDir = validateDataDir(configMap[DataDir])
	configuration.DeploymentType = checkDeploymentType()
	configuration.ClusterNodeCount = validateClusterNodeCount(configMap[ClusterNodeCount], configuration.IsControllerCandidate, configuration.DeploymentType)
	configuration.ControllerCandidateServers = validateControllerCandidateServers(configMap[ControllerCandidateServers], configuration.DeploymentType)
	configuration.HeartbeatCheckInterval = validateHeartbeatCheckInterval(configMap[HeartbeatCheckInterval])
	configuration.HeartbeatTimeoutPeriod = validateHeartbeatTimeoutPeriod(configMap[HeartbeatTimeoutPeriod])
}

// 校验节点id参数
func validateNodeId(nodeId any) int32 {
	return utils.ValidateIntType(nodeId, NodeId)
}

// 校验节点ip地址
func validateNodeIp(nodeIp any) string {
	nodeIpStr := utils.ValidateStringType(nodeIp, NodeIp)
	utils.ValidateRegexp(nodeIpStr, "(\\d+\\.\\d+\\.\\d+\\.\\d+)", "["+NodeIp+"] The parameter must comply with the ip address specification!")
	return nodeIpStr
}

// 校验节点内部通信TCP端口号
func validateNodeInternTcpPort(nodeInternTcpPort any) int32 {
	return utils.ValidateIntType(nodeInternTcpPort, NodeInternTcpPort)
}

// 校验节跟客户端通信的HTTP端口号
func validateNodeClientHttpPort(nodeClientHttpPort any) int32 {
	return utils.ValidateIntType(nodeClientHttpPort, NodeClientHttpPort)
}

// 校验跟客户端通信的TCP端口号
func validateNodeClientTcpPort(nodeClientTcpPort any) int32 {
	return utils.ValidateIntType(nodeClientTcpPort, NodeClientTcpPort)
}

// 校验是否为controller候选节点的参数
func validateIsControllerCandidate(isControllerCandidate any) bool {
	nodeClientTcpPortStr := utils.ValidateStringType(isControllerCandidate, IsControllerCandidate)
	if nodeClientTcpPortStr == "true" || nodeClientTcpPortStr == "false" {
		nodeClientTcpPortBool, err := strconv.ParseBool(nodeClientTcpPortStr)
		if err != nil {
			fmt.Printf("parse %v [%v] to int type error, err: %v", IsControllerCandidate, nodeClientTcpPortStr, err)
			os.Exit(1)
		}
		return nodeClientTcpPortBool
	}
	fmt.Printf("[%v] The parameter must be is 'true' or 'false'!", IsControllerCandidate)
	os.Exit(1)
	return false
}

// 校验集群节点总数量
func validateClusterNodeCount(clusterNodesCount any, isControllerCandidate bool, deploymentType DeploymentType) int32 {
	if isControllerCandidate && deploymentType == Cluster {
		return utils.ValidateIntType(clusterNodesCount, ClusterNodeCount)
	}
	return 1
}

// 校验数据存储目录配置项
func validateDataDir(dataDir any) string {
	return utils.ValidateStringType(dataDir, DataDir)
}

// check deployment type
func checkDeploymentType() DeploymentType {
	otherControllerCandidateServers := GetOtherControllerCandidateServers()
	if len(otherControllerCandidateServers) == 0 {
		return Single
	}
	return Cluster
}

// 校验controller候选节点机器列表
func validateControllerCandidateServers(controllerCandidateServers any, deploymentType DeploymentType) string {
	if Single == deploymentType {
		return ""
	}
	controllerCandidateServersStr := utils.ValidateStringType(controllerCandidateServers, ControllerCandidateServers)
	controllerCandidateServersStrSplit := strings.Split(controllerCandidateServersStr, ",")
	for _, controllerCandidateServer := range controllerCandidateServersStrSplit {
		utils.ValidateRegexp(controllerCandidateServer, "(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)", "["+ControllerCandidateServers+"] The format of the parameter is incorrect")
	}
	return controllerCandidateServersStr
}

// 校验心跳检查时间间隔
func validateHeartbeatCheckInterval(heartbeatCheckInterval any) int32 {
	if heartbeatCheckInterval == nil {
		return DefaultHeartbeatCheckInterval
	} else {
		return utils.ValidateIntType(heartbeatCheckInterval, HeartbeatCheckInterval)
	}
}

// 校验心跳超时时间
func validateHeartbeatTimeoutPeriod(heartbeatTimeoutPeriod any) int32 {
	if heartbeatTimeoutPeriod == nil {
		return DefaultHeartbeatTimeoutPeriod
	} else {
		return utils.ValidateIntType(heartbeatTimeoutPeriod, HeartbeatTimeoutPeriod)
	}
}

func PrintConfigLog() {
	configuration := GetConfigurationInstance()
	log.Info.Printf("[%s]=%d", NodeId, configuration.NodeId)
	log.Info.Printf("[%s]=%s", NodeIp, configuration.NodeIp)
	log.Info.Printf("[%s]=%d", NodeInternTcpPort, configuration.NodeInternTcpPort)
	log.Info.Printf("[%s]=%d", NodeClientHttpPort, configuration.nodeClientHttpPort)
	log.Info.Printf("[%s]=%d", NodeClientTcpPort, configuration.NodeClientTcpPort)
	log.Info.Printf("[%s]=%t", IsControllerCandidate, configuration.IsControllerCandidate)
	log.Info.Printf("[%s]=%d", ClusterNodeCount, configuration.ClusterNodeCount)
	log.Info.Printf("[%s]=%s", DataDir, configuration.DataDir)
	log.Info.Printf("[%s]=%s", ControllerCandidateServers, configuration.ControllerCandidateServers)
	log.Info.Printf("[%s]=%s", "deployment.type", configuration.DeploymentType)
	log.Info.Printf("[%s]=%d", HeartbeatCheckInterval, configuration.HeartbeatCheckInterval)
	log.Info.Printf("[%s]=%d", HeartbeatTimeoutPeriod, configuration.HeartbeatTimeoutPeriod)
}

func IsClusterDeployment() bool {
	return GetConfigurationInstance().DeploymentType == Cluster
}

// GetOtherControllerCandidateServers 获取除自己以外的其他controller候选节点的地址
func GetOtherControllerCandidateServers() (otherControllerCandidateServers []string) {
	configuration := GetConfigurationInstance()
	controllerCandidateServers := configuration.ControllerCandidateServers
	if len(controllerCandidateServers) == 0 {
		return
	}
	nodeIp := configuration.NodeIp
	nodeInternTcpPort := configuration.NodeInternTcpPort
	controllerCandidateServersSplit := strings.Split(controllerCandidateServers, ",")

	for _, controllerCandidateServer := range controllerCandidateServersSplit {
		controllerCandidateServerSplit := strings.Split(controllerCandidateServer, ":")
		controllerCandidateInternTcpPort, _ := strconv.ParseInt(controllerCandidateServerSplit[1], 10, 32)

		if !(controllerCandidateServerSplit[0] == nodeIp) ||
			!(int32(controllerCandidateInternTcpPort) == nodeInternTcpPort) {
			otherControllerCandidateServers = append(otherControllerCandidateServers, controllerCandidateServer)
		}
	}
	return
}

// GetBeforeControllerCandidateServers 获取在配置文件的controller候选节点列表里，排在自己前面的节点列表
func GetBeforeControllerCandidateServers() (beforeControllerCandidateServers []string) {
	configuration := GetConfigurationInstance()
	nodeIp := configuration.NodeIp
	nodeInternTcpPort := configuration.NodeInternTcpPort
	controllerCandidateServers := configuration.ControllerCandidateServers

	controllerCandidateServersSplit := strings.Split(controllerCandidateServers, ",")

	for _, controllerCandidateServer := range controllerCandidateServersSplit {
		controllerCandidateServerSplit := strings.Split(controllerCandidateServer, ":")
		controllerCandidateInternTcpPort, _ := strconv.ParseInt(controllerCandidateServerSplit[1], 10, 32)

		if !(controllerCandidateServerSplit[0] == nodeIp) ||
			!(int32(controllerCandidateInternTcpPort) == nodeInternTcpPort) {
			beforeControllerCandidateServers = append(beforeControllerCandidateServers, controllerCandidateServer)
		} else if controllerCandidateServerSplit[0] == nodeIp ||
			int32(controllerCandidateInternTcpPort) == nodeInternTcpPort {
			break
		}
	}
	return
}
