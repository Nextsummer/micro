package core

import (
	"github.com/Nextsummer/micro/client/config"
	"github.com/Nextsummer/micro/pkg/queue"
	cmap "github.com/orcaman/concurrent-map/v2"
	"sync"
)

type ServiceInstance struct {
	responses           cmap.ConcurrentMap[string, string]
	slotsAllocation     cmap.ConcurrentMap[int32, queue.Array[string]]
	servers             cmap.ConcurrentMap[int32, config.Server]
	controllerCandidate string
	server              config.Server
	RunningState
	excludedRemoteAddress string
}

func (s *ServiceInstance) networkIO() {
	for s.IsRunning() {

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
