package cluster

import (
	"ringodis/interface/resp"
	"strings"
)

var router = make(map[string]CmdFunc)

func registerCmd(name string, cmd CmdFunc) {
	name = strings.ToLower(name)
	router[name] = cmd
}

func registerDefaultCmd(name string) {
	registerCmd(name, defaultFunc)
}

func defaultFunc(cluster *Cluster, c resp.Connection, cmdLine [][]byte) resp.Reply {
	key := string(cmdLine[1])
	node := cluster.peerPicker.PickNode(key)
	return cluster.relay(node, c, cmdLine)
}

func init() {
	defaultCmds := []string{
		"expire",
		"ttl",
		"exists",
		"type",
		"set",
		"setNx",
		"setEx",
		"get",
	}
	for _, name := range defaultCmds {
		registerDefaultCmd(name)
	}
}
