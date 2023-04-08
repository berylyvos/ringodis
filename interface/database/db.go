package database

import "ringodis/interface/resp"

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// DB is the interface for redis style storage engine
type DB interface {
	Exec(client resp.Connection, cmdLine CmdLine) resp.Reply
	Close()
	AfterClientClose(c resp.Connection)
}

// DataEntity stores data bound to a key, including a string, list, hash, set, etc.
type DataEntity struct {
	Data interface{}
}
