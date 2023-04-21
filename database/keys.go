package database

import (
	"ringodis/ds/dict"
	"ringodis/interface/resp"
	"ringodis/lib/wildcard"
	"ringodis/resp/reply"
)

// execDel deletes one or more keys
func execDel(db *DB, args CmdArgs) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

// execExists determines whether one or more keys exists
func execExists(db *DB, args CmdArgs) resp.Reply {
	res := int64(0)
	for _, v := range args {
		if _, exists := db.GetEntity(string(v)); exists {
			res++
		}
	}
	return reply.MakeIntReply(res)
}

// execFlushDB removes all keys from the current db
func execFlushDB(db *DB, args CmdArgs) resp.Reply {
	db.Flush()
	return reply.MakeOkReply()
}

// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *DB, args CmdArgs) resp.Reply {
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	case dict.Dict:
		return reply.MakeStatusReply("hash")
		// case list
		// case set
		// case sortedset
	}
	return reply.MakeUnknownErrReply()
}

// execRename renames a key and overwrites the destination
func execRename(db *DB, args CmdArgs) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	return reply.MakeOkReply()
}

// execRenameNx renames a key, only if the new key does not exist
func execRenameNx(db *DB, args CmdArgs) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	if _, ok := db.GetEntity(dest); ok {
		return reply.MakeIntReply(0)
	}
	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	return reply.MakeIntReply(1)
}

// execKeys returns all keys matching the given pattern
func execKeys(db *DB, args CmdArgs) resp.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR illegal wildcard")
	}
	res := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			res = append(res, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(res)
}

func init() {
	RegisterCommand("Del", execDel, -2)
	RegisterCommand("Exists", execExists, -2)
	RegisterCommand("FlushDB", execFlushDB, -1)
	RegisterCommand("Type", execType, 2)
	RegisterCommand("Rename", execRename, 3)
	RegisterCommand("RenameNx", execRenameNx, 3)
	RegisterCommand("Keys", execKeys, 2)

}
