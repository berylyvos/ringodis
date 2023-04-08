package main

import (
	"fmt"
	"ringodis/config"
	"ringodis/lib/logger"
	"ringodis/resp/server"
	"ringodis/tcp"
)

const configFile string = "ringodis.conf"

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "ringodis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	config.SetupConfig(configFile)

	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind,
				config.Properties.Port),
		},
		server.MakeHandler(),
	)
	if err != nil {
		logger.Error(err)
	}
}
