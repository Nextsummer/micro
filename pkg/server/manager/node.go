package manager

import (
	"github.com/Nextsummer/micro/pkg/server/config"
)

func Start() {
	StartServerConnectionListener()

	configuration := config.GetConfigurationInstance()

	if configuration.IsControllerCandidate {
		if !ConnectBeforeControllerCandidateServers() {
			return
		}
		waitAllControllerCandidatesConnected()
		waitAllServerNodeConnected()
	} else {
		ConnectAllControllerCandidates()
	}

}
