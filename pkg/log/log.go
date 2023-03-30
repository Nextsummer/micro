package log

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/server/config"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

func InitLog() {
	//log.SetFormatter(&log.JSONFormatter{})
	//log.SetLevel(log.InfoLevel)
	log.SetOutput(io.MultiWriter(os.Stdout, openFile(fmt.Sprintf("./server-%d.log", config.GetConfigurationInstance().NodeId))))
}

func openFile(path string) (file *os.File) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.WithFields(log.Fields{
			"exceptionFilePath": path,
		}).Error("open file error, error msg: ", err)
	}
	return
}
