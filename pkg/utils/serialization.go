package utils

import (
	"encoding/json"
	"github.com/Nextsummer/micro/pkg/log"
	"google.golang.org/protobuf/proto"
)

func Encode(m proto.Message) []byte {
	bytes, err := proto.Marshal(m)
	if err != nil {
		log.Error.Println("Failed to serialize message, error msg: ", err)
		return nil
	}
	return bytes
}

func Decode(data []byte, messageEntity proto.Message) {
	err := proto.Unmarshal(data, messageEntity)
	if err != nil {
		log.Error.Println("Service message deserialization failed when received, error msg: ", err)
	}
}

func ToJson(x any) string {
	bytes, err := json.Marshal(x)
	if err != nil {
		log.Error.Println("Failed to serialize message, error msg: ", err)
	}
	return string(bytes)
}

func ToJsonByte(x any) []byte {
	bytes, err := json.Marshal(x)
	if err != nil {
		log.Error.Println("Failed to serialize message, error msg: ", err)
	}
	return bytes
}

func BytesToJson(bytes []byte, x any) {
	if string(bytes) == "null" {
		return
	}
	err := json.Unmarshal(bytes, x)
	if err != nil {
		log.Error.Println("Failed to deserialization message, error msg: ", err)
	}
}
