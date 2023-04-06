package log

import (
	"fmt"
	"io"
	"log"
	"os"
)

var (
	Info  log.Logger
	Warn  log.Logger
	Error log.Logger
)

func InitLog(nodeId int32) {
	flags := log.Ldate | log.Lmicroseconds | log.Lshortfile
	output := io.MultiWriter(os.Stdout, openFile(fmt.Sprintf("./logs/server-%d.log", nodeId)))
	Info = *log.New(output, "[INFO] ", flags)
	Warn = *log.New(output, "[Warn] ", flags)
	Error = *log.New(output, "[Error]", flags)
}

func openFile(path string) (file *os.File) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("open write file error, error msg: ", err)
	}
	return
}
