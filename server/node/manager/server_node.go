package manager

import (
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/server/config"
)

func Start() {
	StartServerConnectionListener()

	configuration := config.GetConfigurationInstance()
	isControllerCandidate := configuration.IsControllerCandidate
	if isControllerCandidate {
		if !ConnectBeforeControllerCandidateServers() {
			return
		}
		waitAllControllerCandidatesConnected()
		waitAllServerNodeConnected()
	} else {
		ConnectAllControllerCandidates()
	}

	// Start the message receiving component of the server node
	go GetServerMessageReceiverInstance().run()

	// Determines whether you are a controller candidate
	isController := false
	serverNodeRole := commonNode
	if isControllerCandidate {
		// Vote for the election controller
		controllerCandidate := GetControllerCandidateInstance()

		serverNodeManager := GetRemoteServerNodeManagerInstance()
		if serverNodeManager.hasController() {
			controller := serverNodeManager.getController()
			requestSlotsData(controller.GetNodeId())
			controllerCandidate.waitForSlotsAllocation()
			controllerCandidate.waitForSlotsReplicaAllocation()
			controllerCandidate.waitReplicaNodeIds()
		} else {
			serverNodeRole = controllerCandidate.electController()
			log.Info.Println("The role obtained by election is: ", getServerNodeRole(serverNodeRole))

			if serverNodeRole == controller {
				isController = true
				controller := getControllerInstance()
				controller.allocateSlots()
				controller.initControllerNode()
				sendControllerNodeId()
			} else if serverNodeRole == candidate {
				controllerCandidate.waitForSlotsAllocation()
				controllerCandidate.waitForSlotsReplicaAllocation()
				controllerCandidate.waitReplicaNodeIds()
			}
		}
	}

	if !isController {
		slotManager := GetSlotManagerInstance()
		slotManager.initSlots(nil)
		slotManager.initSlotsReplicas(nil, false)
		slotManager.initReplicaNodeId(0)
		for {
			controllerNodeId, ok := serverMessageReceiver.controllerNodeIdQueue.Take()
			if !ok {
				continue
			}
			GetControllerNodeInstance().SetControllerNodeId(controllerNodeId)
			break
		}
	}
	SetServerNodeRole(serverNodeRole)

	go ReplicationManager()
	StartClientIO()
}
