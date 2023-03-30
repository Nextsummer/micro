package main

import (
	"flag"
	pklog "github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/pkg/server/config"
	"github.com/Nextsummer/micro/pkg/server/manager"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	var configPath = ""
	flag.StringVar(&configPath, "configPath", "", "config path")
	flag.Parse()
	config.Parse(configPath)
	pklog.InitLog()

	log.Infoln("Starting the Micro platform....")

	manager.Running()
	manager.Start()

	log.Infoln("The micro-service platform Server has been started!")
	for true {
		time.Sleep(time.Second)
	}

}
