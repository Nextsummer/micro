package persist

import (
	"github.com/Nextsummer/micro/pkg/config"
	"github.com/Nextsummer/micro/pkg/log"
	"os"
)

// Persist Persistent slots allocate data to local disks
func Persist(bytes []byte, filename string) bool {
	configuration := config.GetConfigurationInstance()
	dataDir := configuration.DataDir

	var f *os.File
	var err error
	err = os.MkdirAll(dataDir, os.ModePerm)
	f, err = os.OpenFile(dataDir+filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	defer f.Close()
	if err != nil {
		log.Error.Println("persist file create or open error: ", err)
		return false
	}

	_, err = f.Write(bytes)
	if err != nil {
		log.Error.Println("persist file write slots allocation error: ", err)
		return false
	}

	err = f.Sync()
	if err != nil {
		log.Error.Println("persist slots allocation error: ", err)
		return false
	}
	return true
}
