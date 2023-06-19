package zset

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRandomLevel(t *testing.T) {
	m := make(map[int16]int)
	for i := 0; i < 100000; i++ {
		level := randomLevel()
		m[level]++
	}
	for i := 0; i <= maxLevel; i++ {
		t.Logf("level %d, count %d", i, m[int16(i)])
	}
}

func TestInsert(t *testing.T) {
	sl := makeSkiplist()
	insert(sl)
	printSkiplist(sl)
}

func TestRemove(t *testing.T) {
	sl := makeSkiplist()
	insert(sl)
	printSkiplist(sl)
	sl.remove("barr", 2)
	sl.remove("xxx", 42)
	assert.Equal(t, sl.remove("nil", 0), false)
	printSkiplist(sl)
}

func insert(sl *skiplist) {
	sl.insert("xxx", 42)
	sl.insert("abed", 1)
	sl.insert("byre", 2)
	sl.insert("barr", 2)
	sl.insert("troy", 1)
}

func printSkiplist(sl *skiplist) {
	for n := sl.head.level[0].next; n != nil; {
		fmt.Printf("[%v, %v] -> ", n.Member, n.Score)
		n = n.level[0].next
	}
	fmt.Println()
}
