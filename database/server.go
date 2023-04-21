package database

import (
	"fmt"
	"ringodis/config"
	"ringodis/interface/resp"
	"ringodis/lib/logger"
	"ringodis/resp/reply"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
)

type Server struct {
	dbSet []*atomic.Value // *DB
}

// NewStandaloneServer creates a standalone redis server, with multi database and all other functions
func NewStandaloneServer() *Server {
	server := &Server{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	return server
}

// Exec executes command
// parameter `cmdLine` contains command name and its arguments, for example: "set key value"
func (server *Server) Exec(client resp.Connection, cmdLine CmdLine) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = reply.MakeUnknownErrReply()
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(client, server, cmdLine[1:])
	}

	dbIndex := client.GetDBIndex()
	selectDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectDB.Exec(client, cmdLine)
}

func (server *Server) Close() {
	//TODO implement me
	panic("implement me")
}

func (server *Server) AfterClientClose(c resp.Connection) {
	//TODO implement me
	panic("implement me")
}

func execSelect(c resp.Connection, s *Server, args CmdArgs) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(s.dbSet) || dbIndex < 0 {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}

func (server *Server) selectDB(dbIndex int) (*DB, *reply.StandardErrReply) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, reply.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}
