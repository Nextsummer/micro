package manager

import (
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/server/config"
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
	go getServerMessageReceiverInstance().run()

	// Determines whether you are a controller candidate
	serverNodeRole := commonNode
	if isControllerCandidate {
		// Vote for the election controller
		controllerCandidate := GetControllerCandidateInstance()

		serverNodeRole = controllerCandidate.electController()
		log.Info.Println("The role obtained by election is: ", getServerNodeRole(serverNodeRole))

	}

}
