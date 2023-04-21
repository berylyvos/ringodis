package database

import (
	"ringodis/lib/utils"
	"ringodis/resp/reply"
	"ringodis/resp/reply/asserts"
	"testing"
)

var testDB = makeDB()

func TestExists(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 1)
	key = utils.RandString(10)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 0)
}

func TestType(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("type", key))
	asserts.AssertStatusReply(t, result, "string")

	/*
		testDB.Remove(key)
		result = testDB.Exec(nil, utils.ToCmdLine("type", key))
		asserts.AssertStatusReply(t, result, "none")
		execRPush(testDB, utils.ToCmdLine(key, value))
		result = testDB.Exec(nil, utils.ToCmdLine("type", key))
		asserts.AssertStatusReply(t, result, "list")

		testDB.Remove(key)
		testDB.Exec(nil, utils.ToCmdLine("hset", key, key, value))
		result = testDB.Exec(nil, utils.ToCmdLine("type", key))
		asserts.AssertStatusReply(t, result, "hash")

		testDB.Remove(key)
		testDB.Exec(nil, utils.ToCmdLine("sadd", key, value))
		result = testDB.Exec(nil, utils.ToCmdLine("type", key))
		asserts.AssertStatusReply(t, result, "set")

		testDB.Remove(key)
		testDB.Exec(nil, utils.ToCmdLine("zadd", key, "1", value))
		result = testDB.Exec(nil, utils.ToCmdLine("type", key))
		asserts.AssertStatusReply(t, result, "zset")
	*/
}

func TestRename(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))
	result := testDB.Exec(nil, utils.ToCmdLine("rename", key, newKey))
	if _, ok := result.(*reply.OkReply); !ok {
		t.Error("expect ok")
		return
	}
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	//result = testDB.Exec(nil, utils.ToCmdLine("ttl", newKey))
	//intResult, ok := result.(*reply.IntReply)
	//if !ok {
	//	t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
	//	return
	//}
	//if intResult.Code <= 0 {
	//	t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
	//	return
	//}
}

func TestRenameNx(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))
	result := testDB.Exec(nil, utils.ToCmdLine("RenameNx", key, newKey))
	asserts.AssertIntReply(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", newKey))
	asserts.AssertIntReply(t, result, 1)
	//result = testDB.Exec(nil, utils.ToCmdLine("ttl", newKey))
	//intResult, ok := result.(*reply.IntReply)
	//if !ok {
	//	t.Error(fmt.Sprintf("expected int reply, actually %s", result.ToBytes()))
	//	return
	//}
	//if intResult.Code <= 0 {
	//	t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
	//	return
	//}
}

func TestKeys(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "a:"+key, value))
	testDB.Exec(nil, utils.ToCmdLine("set", "b:"+key, value))

	result := testDB.Exec(nil, utils.ToCmdLine("keys", "*"))
	asserts.AssertMultiBulkReplySize(t, result, 3)
	result = testDB.Exec(nil, utils.ToCmdLine("keys", "a:*"))
	asserts.AssertMultiBulkReplySize(t, result, 1)
	result = testDB.Exec(nil, utils.ToCmdLine("keys", "?:*"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
}
