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
		span int64 // skip span - 1 nodes to next
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

func (sl *skiplist) insert(member string, score float64) *node {
	update := make([]*node, maxLevel)
	rank := make([]int64, maxLevel)

	// search for insert position, store prev nodes in update
	cur := sl.head
	rank[sl.lv-1] = 0
	for i := sl.lv - 1; i >= 0; i-- {
		rank[i] = rank[i+1]
		if cur.level[i] != nil {
			for cur.level[i].next != nil &&
				(cur.level[i].next.Score < score ||
					(cur.level[i].next.Score == score &&
						cur.level[i].next.Member < member)) {
				rank[i] += cur.level[i].span
				cur = cur.level[i].next
			}
		}
		update[i] = cur
	}

	newLv := randomLevel()
	// extend levels
	if newLv > sl.lv {
		for i := sl.lv; i < newLv; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].level[i].span = sl.length
		}
		sl.lv = newLv
	}

	// make node and link into skiplist
	cur = makeNode(newLv, score, member)
	for i := int16(0); i < newLv; i++ {
		cur.level[i].next = update[i].level[i].next
		update[i].level[i].next = cur

		cur.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}

	// increment span for untouched levels
	for i := newLv; i < sl.lv; i++ {
		update[i].level[i].span++
	}

	// update cur & next node's prev pointer
	if update[0] == sl.head {
		cur.prev = nil
	} else {
		cur.prev = update[0]
	}
	if cur.level[0].next != nil {
		cur.level[0].next.prev = cur
	} else {
		sl.tail = cur
	}

	sl.length++
	return cur
}

func (sl *skiplist) remove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	cur := sl.head
	for i := sl.lv - 1; i >= 0; i-- {
		for cur.level[i].next != nil &&
			(cur.level[i].next.Score < score ||
				(cur.level[i].next.Score == score &&
					cur.level[i].next.Member < member)) {
			cur = cur.level[i].next
		}
		update[i] = cur
	}
	cur = cur.level[0].next
	if cur != nil && cur.Score == score && cur.Member == member {
		sl.removeNode(cur, update)
		return true
	}
	return false
}

func (sl *skiplist) removeNode(node *node, update []*node) {
	for i := int16(0); i < sl.lv; i++ {
		if update[i].level[i].next == node {
			update[i].level[i].next = node.level[i].next
			update[i].level[i].span += node.level[i].span + 1
		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].next != nil {
		node.level[0].next.prev = node.prev
	} else {
		sl.tail = node.prev
	}
	if sl.lv > 1 && sl.head.level[sl.lv-1].next == nil {
		sl.lv--
	}
	sl.length--
}

func (sl *skiplist) getByRank(rank int64) *node {
	// rank start at 1, head rank is 0
	var rk int64 = 0
	cur := sl.head
	for lv := sl.lv - 1; lv >= 0; lv-- {
		// scan in current level, if over rank, down to next level
		for cur.level[lv].next != nil && (rk+cur.level[lv].span) <= rank {
			rk += cur.level[lv].span
			cur = cur.level[lv].next
		}
		if rk == rank {
			return cur
		}
	}
	return nil
}
