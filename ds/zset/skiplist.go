package zset

import (
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 16
)

type (
	Elem struct {
		Member string
		Score  float64
	}

	Level struct {
		next *node // next has greater Elem.Score
		span int64
	}

	node struct {
		Elem
		prev  *node
		level []*Level // level[0] is base Level
	}

	skiplist struct {
		head   *node
		tail   *node
		length int64
		lv     int16
	}
)

func makeNode(lv int16, score float64, member string) *node {
	n := &node{
		Elem: Elem{
			Member: member,
			Score:  score,
		},
		level: make([]*Level, lv),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		head: makeNode(maxLevel, 0, ""),
		lv:   1,
	}
}

func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}
