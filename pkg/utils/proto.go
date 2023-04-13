package utils

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/Nextsummer/micro/pkg/log"
	"google.golang.org/protobuf/proto"
	"io"
)

func Encode(m proto.Message) []byte {
	bytes, err := proto.Marshal(m)
	if err != nil {
		log.Error.Println("Failed to serialize message, error msg: ", err)
		return nil
	}
	return bytes
}

func Decode(data []byte, messageEntity proto.Message) error {
	err := proto.Unmarshal(data, messageEntity)
	if err != nil {
		log.Warn.Println("Service message deserialization failed when received, error msg: ", err)
		return err
	}
	return nil
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

func TcpEncode(message []byte) ([]byte, error) {
	var length = int32(len(message))
	var pkg = new(bytes.Buffer)

	err := binary.Write(pkg, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}
	err = binary.Write(pkg, binary.LittleEndian, message)
	if err != nil {
		return nil, err
	}
	return pkg.Bytes(), nil
}

func TcpDecode(reader *bufio.Reader) ([]byte, error) {
	var length int32

	lengthByte, _ := reader.Peek(4)
	err := binary.Read(bytes.NewBuffer(lengthByte), binary.LittleEndian, &length)
	if err != nil {
		return nil, err
	}

	if int32(reader.Buffered()) < length+4 {
		return nil, err
	}

	pack := make([]byte, int(4+length))
	_, err = reader.Read(pack)
	if err != nil {
		return nil, err
	}

	return pack[4:], nil
}

func ReadByte(rd io.Reader) ([]byte, error) {
	reader := bufio.NewReader(rd)
	pack := make([]byte, reader.Size())
	_, err := reader.Read(pack)
	if err != nil {
		return nil, err
	}
	return pack, nil
}
