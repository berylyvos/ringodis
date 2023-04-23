package database

import (
	"ringodis/interface/database"
	"ringodis/interface/resp"
	"ringodis/resp/reply"
	"strconv"
	"strings"
)

const (
	unlimitedTTL int64 = 0
)

func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

// execGet returns string value bound to the given key
func execGet(db *DB, args CmdArgs) resp.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeBulkReply(bytes)
}

// execSet sets string value and time to live to the given key
// SET key value [EX seconds]
func execSet(db *DB, args CmdArgs) resp.Reply {
	key := string(args[0])
	val := args[1]
	ttl := unlimitedTTL

	if len(args) > 2 {
		if arg := strings.ToUpper(string(args[2])); arg == "EX" {
			ttlSec, err := strconv.ParseInt(arg, 10, 64)
			if err != nil {
				return reply.MakeSyntaxErrReply()
			}
			if ttlSec <= 0 {
				return reply.MakeErrReply("ERR invalid expire time in set")
			}
			ttl = ttlSec
		} else {
			return reply.MakeSyntaxErrReply()
		}
	}

	entity := &database.DataEntity{
		Data: val,
	}
	db.PutEntity(key, entity)

	if ttl != unlimitedTTL {
		db.Expire(key, calcExpireTime(ttl))
	} else {
		db.Persist(key)
	}

	return reply.MakeOkReply()
}

// execSetNX sets string value if key not exists
func execSetNX(db *DB, args CmdArgs) resp.Reply {
	key := string(args[0])
	val := args[1]
	entity := &database.DataEntity{
		Data: val,
	}
	res := db.PutIfAbsent(key, entity)
	return reply.MakeIntReply(int64(res))
}

// execSetEX sets string and its ttl
// SETEX key seconds value
func execSetEX(db *DB, args CmdArgs) resp.Reply {
	key := string(args[0])
	val := args[2]
	entity := &database.DataEntity{
		Data: val,
	}

	ttlSec, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeSyntaxErrReply()
	}
	if ttlSec <= 0 {
		return reply.MakeErrReply("ERR invalid expire time in setex")
	}

	db.PutEntity(key, entity)
	db.Expire(key, calcExpireTime(ttlSec))
	return reply.MakeOkReply()
}

// execGetSet sets value of a string-type key and returns its old value
func execGetSet(db *DB, args CmdArgs) resp.Reply {
	key := string(args[0])
	val := args[1]

	old, err := db.getAsString(key)
	if err != nil {
		return err
	}
	db.PutEntity(key, &database.DataEntity{Data: val})
	if old == nil {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeBulkReply(old)
}

// execStrLen returns len of string value bound to the given key
func execStrLen(db *DB, args CmdArgs) resp.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("Get", execGet, 2)
	RegisterCommand("Set", execSet, -3)
	RegisterCommand("SetNX", execSetNX, 3)
	RegisterCommand("GetSet", execGetSet, 3)
	RegisterCommand("StrLen", execStrLen, 2)
	RegisterCommand("SetEX", execSetEX, 4)
}
