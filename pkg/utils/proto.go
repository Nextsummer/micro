package utils

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/Nextsummer/micro/pkg/log"
	"google.golang.org/protobuf/proto"
)

func GrpcEncode(m proto.Message) []byte {
	bytes, err := proto.Marshal(m)
	if err != nil {
		log.Error.Println("Failed to serialize message, error msg: ", err)
		return nil
	}
	return bytes
}

func GrpcDecode(data []byte, messageEntity proto.Message) {
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

func HttpEncode(flag, messageType int32, message []byte) ([]byte, error) {
	var length = int32(len(message))
	var pkg = new(bytes.Buffer)

	err := binary.Write(pkg, binary.LittleEndian, flag)
	if err != nil {
		return nil, err
	}
	err = binary.Write(pkg, binary.LittleEndian, messageType)
	if err != nil {
		return nil, err
	}

	err = binary.Write(pkg, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}
	err = binary.Write(pkg, binary.LittleEndian, message)
	if err != nil {
		return nil, err
	}
	return pkg.Bytes(), nil
}

func HttpDecode(reader *bufio.Reader) (int32, int32, []byte, error) {
	var flag, messageType, length int32

	flagByte, _ := reader.Peek(4)
	err := binary.Read(bytes.NewBuffer(flagByte), binary.LittleEndian, &flag)
	if err != nil {
		return 0, 0, nil, err
	}

	messageTypeByte, _ := reader.Peek(4 + 4)
	err = binary.Read(bytes.NewBuffer(messageTypeByte), binary.LittleEndian, &messageType)
	if err != nil {
		return 0, 0, nil, err
	}

	lengthByte, _ := reader.Peek(4 + 4 + 4)
	err = binary.Read(bytes.NewBuffer(lengthByte), binary.LittleEndian, &length)
	if err != nil {
		return 0, 0, nil, err
	}
	if int32(reader.Buffered()) < length+4+4+4 {
		return 0, 0, nil, err
	}

	pack := make([]byte, int(4+length))
	_, err = reader.Read(pack)
	if err != nil {
		return 0, 0, nil, err
	}
	return flag, messageType, pack[4+4+4:], nil
}
