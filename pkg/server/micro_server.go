package main

import (
	"flag"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/server/config"
	"github.com/Nextsummer/micro/pkg/server/node/manager"
	"time"
)

func main() {
	var configPath = ""
	flag.StringVar(&configPath, "configPath", "", "config path")
	flag.Parse()
	config.Parse(configPath)

	log.InitLog(config.GetConfigurationInstance().NodeId)
	config.PrintConfigLog()

	log.Info.Println("Starting the Micro platform....")

	manager.Running()
	manager.Start()

	log.Info.Println("The micro-service platform Server has been started!")
	for manager.IsRunning() {
		time.Sleep(time.Second)
	}

}
