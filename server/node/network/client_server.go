package network

import (
	"fmt"
	"github.com/Nextsummer/micro/pkg/log"
	"github.com/Nextsummer/micro/server/config"
	"github.com/Nextsummer/micro/server/node/manager"
	"net"
)

func startClientIO() {
	clientTcpPort := config.GetConfigurationInstance().NodeClientTcpPort

	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", clientTcpPort))
	if err != nil {
		log.Error.Fatalf("Client io server binding to port [%d] error, error msg: %v", clientTcpPort, err)
	}
	go networkIO(listen)
}

func networkIO(listen net.Listener) {
	defer listen.Close()

	for manager.IsRunning() {
		conn, err := listen.Accept()
		if err != nil {
			log.Error.Println("Client io server accept failed, err: ", err)
			continue
		}
		go process(conn)
	}
}

func process(conn net.Conn) {
	defer conn.Close()
	//reader := bufio.NewReader(conn)
	//for {
	//	flag, messageType, message, err := utils.HttpDecode(reader)
	//	if err == io.EOF {
	//		return
	//	}
	//	if err != nil {
	//		log.Error.Println("Client io server decode message failed, err: ", err)
	//		return
	//	}
	//	//todo 待处理，根据flag和messageType找到具体的消息类型
	//
	//}
}
