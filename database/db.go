package database

import (
	"ringodis/ds/dict"
	"ringodis/interface/database"
	"ringodis/interface/resp"
	"ringodis/resp/reply"
	"strings"
)

const (
	dataDictSize = 1 << 16 // 65536
)

// DB stores data and execute user's commands
type DB struct {
	index int
	data  dict.Dict
}

// ExecFunc is interface for command executor
type ExecFunc func(db *DB, args CmdArgs) resp.Reply

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// CmdArgs is alias for [][]byte, represents a command line exclude command name
type CmdArgs = [][]byte

func makeDB() *DB {
	return &DB{
		data: dict.MakeConcurrent(dataDictSize),
	}
}

// Exec executes command within one database
func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {

	return db.execRegularCommand(cmdLine)
}

func (db *DB) execRegularCommand(cmdLine CmdLine) resp.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	return cmd.executor(db, cmdLine[1:])
}

func validateArity(arity int, cmdLine CmdLine) bool {
	argNum := len(cmdLine)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Data Access ----- */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, exists := db.data.Get(key)
	if !exists {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into db
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		if _, exists := db.data.Get(key); exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.data.Clear()
}
