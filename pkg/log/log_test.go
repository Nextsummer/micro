package log

import (
	"testing"
)

func TestInitLog(t *testing.T) {
	InitLog(1)

	Info.Println("test 11")
	Info.Println("test warn 111")
	Info.Println("test warn 111")
	Info.Println("test error 111")
}
