package database

import (
	"ringodis/ds/dict"
	"ringodis/ds/lock"
	"ringodis/interface/database"
	"ringodis/interface/resp"
	"ringodis/lib/logger"
	"ringodis/lib/timewheel"
	"ringodis/resp/reply"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16 // 65536
	ttlDictSize  = 1 << 10 // 1024
	lockerSize   = 1 << 10 // 1024
)

// DB stores data and execute user's commands
type DB struct {
	index int
	// key -> DataEntity
	data dict.Dict
	// key -> expireTime (time.Time)
	ttlMap dict.Dict

	// use locker for complicated command only, e.g. rpush, incr ...
	locker *lock.Locks
}

// ExecFunc is interface for command executor
type ExecFunc func(db *DB, args CmdArgs) resp.Reply

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// CmdArgs is alias for [][]byte, represents a command line exclude command name
type CmdArgs = [][]byte

// PreFunc analyses command line when queued command to `multi`
// returns related writer and reader keys
type PreFunc func(args CmdArgs) ([]string, []string)

func makeDB() *DB {
	return &DB{
		data:   dict.MakeConcurrent(dataDictSize),
		ttlMap: dict.MakeConcurrent(ttlDictSize),
		locker: lock.Make(lockerSize),
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
    
    writerKeys, readerKeys := cmd.prepare(cmdLine[1:])
    db.RWLocks(writerKeys, readerKeys)
    defer db.RWUnLocks(writerKeys, readerKeys)
	return cmd.executor(db, cmdLine[1:])
}

func validateArity(arity int, cmdLine CmdLine) bool {
	argNum := len(cmdLine)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ==== Data Access ==== */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, exists := db.data.Get(key)
	if !exists || db.IsExpired(key) {
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
	db.ttlMap.Remove(key)
	timewheel.Cancel(genExpireTask(key))
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
	db.ttlMap.Clear()
	db.locker = lock.Make(lockerSize)
}

/* ==== Lock Function ==== */

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writerKeys, readerKeys []string) {
	db.locker.RWLocks(writerKeys, readerKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writerKeys, readerKeys []string) {
	db.locker.RWUnLocks(writerKeys, readerKeys)
}

/* ==== TTL Functions ==== */

func genExpireTask(key string) string {
	return "expire:" + key
}

func calcExpireTime(ttlSec int64) time.Time {
	return time.Now().Add(time.Duration(ttlSec*1000) * time.Millisecond)
}

// Expire sets ttlCmd of a key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	// set cron job using time wheel, key will be deleted when expire
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, exists := db.ttlMap.Get(key)
		if !exists {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		if time.Now().After(expireTime) {
			db.Remove(key)
		}
	})
}

// Persist cancel ttlCmd of a key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	timewheel.Cancel(genExpireTask(key))
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, exists := db.ttlMap.Get(key)
	if !exists {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	if time.Now().After(expireTime) {
		db.Remove(key)
		return true
	}
	return false
}
