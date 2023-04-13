package lock

import (
	"sort"
	"sync"
)

const (
	prime32 = uint32(16777619)
)

// Locks provides reader-writer locks for key(s)
type Locks struct {
	m []*sync.RWMutex
}

func Make(size int) *Locks {
	m := make([]*sync.RWMutex, size)
	for i := 0; i < size; i++ {
		m[i] = &sync.RWMutex{}
	}
	return &Locks{
		m: m,
	}
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (ls *Locks) spread(hashCode uint32) uint32 {
	if ls == nil {
		panic("dict is nil")
	}
	return (uint32(len(ls.m)) - 1) & hashCode
}

// Lock obtains exclusive lock for writing
func (ls *Locks) Lock(key string) {
	ls.m[ls.spread(fnv32(key))].Lock()
}

// RLock obtains shared lock for reading
func (ls *Locks) RLock(key string) {
	ls.m[ls.spread(fnv32(key))].RLock()
}

// UnLock releases exclusive lock
func (ls *Locks) UnLock(key string) {
	ls.m[ls.spread(fnv32(key))].Unlock()
}

// RUnLock releases shared lock
func (ls *Locks) RUnLock(key string) {
	ls.m[ls.spread(fnv32(key))].RUnlock()
}

// toLockIndices returns a slice of indices in given order.
// invoked before obtain/release multiple locks to avoid deadlock
func (ls *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexSet := make(map[uint32]struct{})
	for _, key := range keys {
		indexSet[ls.spread(fnv32(key))] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexSet))
	for index := range indexSet {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// Locks obtains multiple exclusive locks for writing
func (ls *Locks) Locks(keys ...string) {
	indices := ls.toLockIndices(keys, false)
	for _, index := range indices {
		ls.m[index].Lock()
	}
}

// RLocks obtains multiple shared locks for reading
func (ls *Locks) RLocks(keys ...string) {
	indices := ls.toLockIndices(keys, false)
	for _, index := range indices {
		ls.m[index].RLock()
	}
}

// UnLocks releases multiple exclusive locks
func (ls *Locks) UnLocks(keys ...string) {
	indices := ls.toLockIndices(keys, true)
	for _, index := range indices {
		ls.m[index].Unlock()
	}
}

// RUnLocks releases multiple shared locks
func (ls *Locks) RUnLocks(keys ...string) {
	indices := ls.toLockIndices(keys, true)
	for _, index := range indices {
		ls.m[index].RUnlock()
	}
}

// RWLocks locks writer keys and reader keys together, allowing duplicate keys
func (ls *Locks) RWLocks(writerKeys, readerKeys []string) {
	keys := append(writerKeys, readerKeys...)
	indices := ls.toLockIndices(keys, false)
	writerIndices := make(map[uint32]struct{})
	for _, wk := range writerKeys {
		writerIndices[ls.spread(fnv32(wk))] = struct{}{}
	}
	for _, index := range indices {
		if _, w := writerIndices[index]; w {
			ls.m[index].Lock()
		} else {
			ls.m[index].RLock()
		}
	}
}

// RWUnLocks unlocks writer keys and reader keys together, allowing duplicate keys
func (ls *Locks) RWUnLocks(writerKeys, readerKeys []string) {
	keys := append(writerKeys, readerKeys...)
	indices := ls.toLockIndices(keys, true)
	writerIndices := make(map[uint32]struct{})
	for _, wk := range writerKeys {
		writerIndices[ls.spread(fnv32(wk))] = struct{}{}
	}
	for _, index := range indices {
		if _, w := writerIndices[index]; w {
			ls.m[index].Unlock()
		} else {
			ls.m[index].RUnlock()
		}
	}
}
