package config

import (
	"bufio"
	"github.com/Nextsummer/micro/pkg/log"
	"io"
	"os"
	"regexp"
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
	heartbeatCheckInterval     int32  //心跳检查时间间隔
	heartbeatTimeoutPeriod     int32  //心跳超时时间
}

func GetConfigurationInstance() *Configuration {
	configurationOnce.Do(func() {
		singleton = &Configuration{}
	})
	return singleton
}

func Parse(configPath string) {
	configMap := loadConfigurationFile(configPath)

	configuration := GetConfigurationInstance()
	configuration.NodeId = validateNodeId(configMap[NodeId])
	configuration.NodeIp = validateNodeIp(configMap[NodeIp])
	configuration.NodeInternTcpPort = validateNodeInternTcpPort(configMap[NodeInternTcpPort])
	configuration.nodeClientHttpPort = validateNodeClientHttpPort(configMap[NodeClientHttpPort])
	configuration.NodeClientTcpPort = validateNodeClientTcpPort(configMap[NodeClientTcpPort])
	configuration.IsControllerCandidate = validateIsControllerCandidate(configMap[IsControllerCandidate])
	configuration.DataDir = validateDataDir(configMap[DataDir])
	configuration.ClusterNodeCount = validateClusterNodeCount(configMap[ClusterNodeCount])
	configuration.ControllerCandidateServers = validateControllerCandidateServers(configMap[ControllerCandidateServers])
	configuration.heartbeatCheckInterval = validateHeartbeatCheckInterval(configMap[HeartbeatCheckInterval])
	configuration.heartbeatTimeoutPeriod = validateHeartbeatTimeoutPeriod(configMap[HeartbeatTimeoutPeriod])
}

// 加载配置文件
func loadConfigurationFile(configPath string) map[string]any {
	if len(configPath) <= 0 {
		log.Error.Fatalln("配置文件地址不能为空！！！")
	}

	file, err := os.Open(configPath)
	defer file.Close()

	if err != nil {
		log.Error.Fatalf("config file read error, config path [%v], err: %v", configPath, err)
	}

	reader := bufio.NewReader(file)
	configMap := make(map[string]any)

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error.Fatalf("config file read error, config path [%v], err: %v", configPath, err)
		}
		lineStr := string(line)
		if len(lineStr) > 0 && len(strings.Split(lineStr, "=")) == 2 {
			lineStrSplit := strings.Split(lineStr, "=")
			configMap[lineStrSplit[0]] = lineStrSplit[1]
		}
	}
	return configMap
}

// 校验节点id参数
func validateNodeId(nodeId any) int32 {
	return validateIntType(nodeId, NodeId)
}

// 校验节点ip地址
func validateNodeIp(nodeIp any) string {
	nodeIpStr := validateStringType(nodeIp, NodeIp)
	validateRegexp(nodeIpStr, "(\\d+\\.\\d+\\.\\d+\\.\\d+)", "["+NodeIp+"] The parameter must comply with the ip address specification!")
	return nodeIpStr
}

// 校验节点内部通信TCP端口号
func validateNodeInternTcpPort(nodeInternTcpPort any) int32 {
	return validateIntType(nodeInternTcpPort, NodeInternTcpPort)
}

// 校验节跟客户端通信的HTTP端口号
func validateNodeClientHttpPort(nodeClientHttpPort any) int32 {
	return validateIntType(nodeClientHttpPort, NodeClientHttpPort)
}

// 校验跟客户端通信的TCP端口号
func validateNodeClientTcpPort(nodeClientTcpPort any) int32 {
	return validateIntType(nodeClientTcpPort, NodeClientTcpPort)
}

// 校验是否为controller候选节点的参数
func validateIsControllerCandidate(isControllerCandidate any) bool {
	nodeClientTcpPortStr := validateStringType(isControllerCandidate, IsControllerCandidate)
	if nodeClientTcpPortStr == "true" || nodeClientTcpPortStr == "false" {
		nodeClientTcpPortBool, err := strconv.ParseBool(nodeClientTcpPortStr)
		if err != nil {
			log.Error.Fatalf("parse %v [%v] to int type error, err: %v", IsControllerCandidate, nodeClientTcpPortStr, err)
		}
		return nodeClientTcpPortBool
	}
	log.Error.Fatalf("[%v] The parameter must be is 'true' or 'false'!", IsControllerCandidate)
	return false
}

// 校验集群节点总数量
func validateClusterNodeCount(clusterNodesCount any) int32 {
	isControllerCandidate := GetConfigurationInstance().IsControllerCandidate
	if isControllerCandidate {
		return validateIntType(clusterNodesCount, ClusterNodeCount)
	}
	return 0
}

// 校验数据存储目录配置项
func validateDataDir(dataDir any) string {
	return validateStringType(dataDir, DataDir)
}

// 校验controller候选节点机器列表
func validateControllerCandidateServers(controllerCandidateServers any) string {
	controllerCandidateServersStr := validateStringType(controllerCandidateServers, ControllerCandidateServers)
	controllerCandidateServersStrSplit := strings.Split(controllerCandidateServersStr, ",")
	for _, controllerCandidateServer := range controllerCandidateServersStrSplit {
		validateRegexp(controllerCandidateServer, "(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)", "["+ControllerCandidateServers+"] The format of the parameter is incorrect")
	}
	return controllerCandidateServersStr
}

// 校验心跳检查时间间隔
func validateHeartbeatCheckInterval(heartbeatCheckInterval any) int32 {
	if heartbeatCheckInterval == nil {
		return DefaultHeartbeatCheckInterval
	} else {
		return validateIntType(heartbeatCheckInterval, HeartbeatCheckInterval)
	}
}

// 校验心跳超时时间
func validateHeartbeatTimeoutPeriod(heartbeatTimeoutPeriod any) int32 {
	if heartbeatTimeoutPeriod == nil {
		return DefaultHeartbeatTimeoutPeriod
	} else {
		return validateIntType(heartbeatTimeoutPeriod, HeartbeatTimeoutPeriod)
	}
}

// 校验数值类型
func validateIntType(data any, errType string) int32 {
	dataStr := validateStringType(data, errType)
	validateRegexp(dataStr, "(\\d+)", "["+errType+"] The parameter must be a number!")

	dataInt, err := strconv.ParseInt(dataStr, 10, 32)
	if err != nil {
		log.Error.Fatalf("parse %v [%v] to int type error, err: %v", errType, dataStr, err)
	}
	return int32(dataInt)
}

func validateStringType(data any, errType string) string {
	dataStr := data.(string)
	if len(dataStr) <= 0 {
		log.Error.Fatalf("[%v] The parameter cannot be null!", errType)
	}
	return dataStr
}

func validateRegexp(data string, regex string, errMsg string) {
	compile, _ := regexp.Compile(regex)
	if !compile.Match([]byte(data)) {
		log.Error.Println(errMsg)
	}
}

func PrintConfigLog() {
	configuration := GetConfigurationInstance()
	log.Info.Printf("[%v]=%v", NodeId, configuration.NodeId)
	log.Info.Printf("[%v]=%v", NodeIp, configuration.NodeIp)
	log.Info.Printf("[%v]=%v", NodeInternTcpPort, configuration.NodeInternTcpPort)
	log.Info.Printf("[%v]=%v", NodeClientHttpPort, configuration.nodeClientHttpPort)
	log.Info.Printf("[%v]=%v", NodeClientTcpPort, configuration.NodeClientTcpPort)
	log.Info.Printf("[%v]=%v", IsControllerCandidate, configuration.IsControllerCandidate)
	log.Info.Printf("[%v]=%v", ClusterNodeCount, configuration.ClusterNodeCount)
	log.Info.Printf("[%v]=%v", DataDir, configuration.DataDir)
	log.Info.Printf("[%v]=%v", ControllerCandidateServers, configuration.ControllerCandidateServers)
	log.Info.Printf("[%v]=%v", HeartbeatCheckInterval, configuration.heartbeatCheckInterval)
	log.Info.Printf("[%v]=%v", HeartbeatTimeoutPeriod, configuration.heartbeatTimeoutPeriod)
}

// GetOtherControllerCandidateServers 获取除自己以外的其他controller候选节点的地址
func GetOtherControllerCandidateServers() (otherControllerCandidateServers []string) {
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
