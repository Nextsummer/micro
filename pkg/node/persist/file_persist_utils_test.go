package persist

import (
	"github.com/Nextsummer/micro/pkg/config"
	"github.com/Nextsummer/micro/pkg/utils"
	"testing"
)

func TestPersist(t *testing.T) {
	config.GetConfigurationInstance().DataDir = ".\\data\\temp"
	bytes := utils.ToJsonByte("hello go!")
	Persist(bytes, "temp2")

}
