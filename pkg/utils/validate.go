package utils

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
)

func ValidateStringType(data any, errType string) string {
	dataStr := data.(string)
	if len(dataStr) <= 0 {
		fmt.Printf("[%v] The parameter cannot be null!", errType)
		os.Exit(1)
	}
	return dataStr
}

func ValidateRegexp(data string, regex string, errMsg string) {
	compile, _ := regexp.Compile(regex)
	if !compile.Match([]byte(data)) {
		fmt.Println(errMsg)
		os.Exit(1)
	}
}

func ValidateIntType(data any, errType string) int32 {
	dataStr := ValidateStringType(data, errType)
	ValidateRegexp(dataStr, "(\\d+)", "["+errType+"] The parameter must be a number!")

	dataInt, err := strconv.ParseInt(dataStr, 10, 32)
	if err != nil {
		fmt.Printf("parse %v [%v] to int type error, err: %v", errType, dataStr, err)
		os.Exit(1)
	}
	return int32(dataInt)
}
