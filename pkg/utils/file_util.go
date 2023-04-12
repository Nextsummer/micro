package utils

import (
	"bufio"
	"github.com/Nextsummer/micro/pkg/log"
	"io"
	"os"
	"strings"
)

func LoadConfigurationFile(configPath string) map[string]any {
	if len(configPath) <= 0 {
		log.Error.Fatalln("配置文件地址不能为空！！！")
	}

	file, err := os.Open(configPath)
	defer file.Close()

	if err != nil {
		log.Error.Fatalf("conf file read error, conf path [%v], err: %v", configPath, err)
	}

	reader := bufio.NewReader(file)
	configMap := make(map[string]any)

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error.Fatalf("conf file read error, conf path [%v], err: %v", configPath, err)
		}
		lineStr := string(line)
		if len(lineStr) > 0 && len(strings.Split(lineStr, "=")) == 2 {
			lineStrSplit := strings.Split(lineStr, "=")
			configMap[lineStrSplit[0]] = lineStrSplit[1]
		}
	}
	return configMap
}
