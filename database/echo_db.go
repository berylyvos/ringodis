package database

import (
	idb "ringodis/interface/database"
	"ringodis/interface/resp"
	"ringodis/resp/reply"
)

type EchoDB struct {
}

func NewEchoDB() *EchoDB {
	return &EchoDB{}
}

func (e *EchoDB) Exec(client resp.Connection, cmdLine idb.CmdLine) resp.Reply {
	return reply.MakeMultiBulkReply(cmdLine)
}

func (e *EchoDB) Close() {
}

func (e *EchoDB) AfterClientClose(c resp.Connection) {
}
