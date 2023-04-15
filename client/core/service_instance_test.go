package core

import (
	"github.com/Nextsummer/micro/pkg/log"
	"testing"
)

func TestServiceInstance_Register(t *testing.T) {
	log.InitLog("/client.log")
	serviceInstance := NewServiceInstance()
	serviceInstance.Init()
	serviceInstance.Register()
}
