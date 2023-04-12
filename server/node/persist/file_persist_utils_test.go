package persist

import (
	"github.com/Nextsummer/micro/pkg/utils"
	"github.com/Nextsummer/micro/server/config"
	"testing"
)

func TestPersist(t *testing.T) {
	config.GetConfigurationInstance().DataDir = ".\\data\\temp"
	bytes := utils.ToJsonByte("hello go!")
	Persist(bytes, "temp2")

}
