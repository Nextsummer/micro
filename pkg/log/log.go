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
	output := io.MultiWriter(os.Stdout, openFile("./logs", fmt.Sprintf("/server-%d.log", nodeId)))
	Info = *log.New(output, "[INFO] ", flags)
	Warn = *log.New(output, "[Warn] ", flags)
	Error = *log.New(output, "[Error]", flags)
}

func openFile(path, filename string) (file *os.File) {
	err := os.MkdirAll(path, os.ModePerm)
	file, err = os.OpenFile(path+filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("open write file error, error msg: ", err)
	}
	return
}
