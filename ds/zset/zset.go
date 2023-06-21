package zset

type ZSet struct {
	dict map[string]*Elem
	sl   *skiplist
}

func Make() *ZSet {
	return &ZSet{
		dict: make(map[string]*Elem),
		sl:   makeSkiplist(),
	}
}

func (zs *ZSet) Add(member string, score float64) bool {
	element, ok := zs.dict[member]
	zs.dict[member] = &Elem{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			zs.sl.remove(member, element.Score)
			zs.sl.insert(member, score)
		}
		return false
	}
	zs.sl.insert(member, score)
	return true
}

func (zs *ZSet) Get(member string) (element *Elem, ok bool) {
	element, ok = zs.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

func (zs *ZSet) Remove(member string) bool {
	v, ok := zs.dict[member]
	if ok {
		zs.sl.remove(member, v.Score)
		delete(zs.dict, member)
		return true
	}
	return false
}

func (zs *ZSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := zs.dict[member]
	if !ok {
		return -1
	}
	r := zs.sl.getRank(member, element.Score)
	if desc {
		r = zs.sl.length - r
	} else {
		r--
	}
	return r
}
