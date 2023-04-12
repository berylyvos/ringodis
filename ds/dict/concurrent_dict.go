package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ConcurrentDict is a thread safe map using sharding lock
type ConcurrentDict struct {
	table      []*shard
	count      int32
	shardCount int
}

type shard struct {
	m   map[string]interface{}
	mut sync.RWMutex
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}

// MakeConcurrent creates a ConcurrentDict with the given shard count
func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	return &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (d *ConcurrentDict) spread(hashCode uint32) uint32 {
	if d == nil {
		panic("dict is nil")
	}
	return (uint32(d.shardCount) - 1) & hashCode
}

func (d *ConcurrentDict) getShard(index uint32) *shard {
	if d == nil {
		panic("dict is nil")
	}
	return d.table[index]
}

func (d *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if d == nil {
		panic("dict is nil")
	}
	s := d.getShard(d.spread(fnv32(key)))
	s.mut.RLock()
	defer s.mut.RUnlock()
	val, exists = s.m[key]
	return
}

func (d *ConcurrentDict) Len() int {
	if d == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&d.count))
}

func (d *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	s := d.getShard(d.spread(fnv32(key)))
	s.mut.Lock()
	defer s.mut.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	d.addCount()
	s.m[key] = val
	return 1
}

func (d *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	s := d.getShard(d.spread(fnv32(key)))
	s.mut.Lock()
	defer s.mut.Unlock()

	if _, ok := s.m[key]; !ok {
		s.m[key] = val
		d.addCount()
		return 1
	}
	return 0
}

func (d *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	s := d.getShard(d.spread(fnv32(key)))
	s.mut.Lock()
	defer s.mut.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (d *ConcurrentDict) Remove(key string) (result int) {
	if d == nil {
		panic("dict is nil")
	}
	s := d.getShard(d.spread(fnv32(key)))
	s.mut.Lock()
	defer s.mut.Unlock()

	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		d.decreaseCount()
		return 1
	}
	return 0
}

func (d *ConcurrentDict) ForEach(consumer Consumer) {
	if d == nil {
		panic("dict is nil")
	}

	for _, s := range d.table {
		s.mut.RLock()
		f := func() bool {
			defer s.mut.RUnlock()
			for key, val := range s.m {
				continues := consumer(key, val)
				if !continues {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

func (d *ConcurrentDict) Keys() []string {
	keys := make([]string, d.Len())
	i := 0
	d.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (s *shard) RandomKey() string {
	if s == nil {
		panic("shard is nil")
	}
	s.mut.RLock()
	defer s.mut.RUnlock()

	for key := range s.m {
		return key
	}
	return ""
}

func (d *ConcurrentDict) RandomKeys(limit int) []string {
	sz := d.Len()
	if limit >= sz {
		return d.Keys()
	}

	sc := d.shardCount
	keys := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := d.getShard(uint32(nR.Intn(sc)))
		if s == nil {
			continue
		}
		if key := s.RandomKey(); key != "" {
			keys[i] = key
			i++
		}
	}
	return keys
}

func (d *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	sz := d.Len()
	if limit >= sz {
		return d.Keys()
	}

	sc := d.shardCount
	km := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(km) < limit {
		s := d.getShard(uint32(nR.Intn(sc)))
		if s == nil {
			continue
		}
		if key := s.RandomKey(); key != "" {
			if _, ok := km[key]; !ok {
				km[key] = struct{}{}
			}
		}
	}
	keys := make([]string, limit)
	i := 0
	for k := range km {
		keys[i] = k
		i++
	}
	return keys
}

func (d *ConcurrentDict) clear() {
	*d = *MakeConcurrent(d.shardCount)
}

func (d *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&d.count, 1)
}

func (d *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&d.count, -1)
}
