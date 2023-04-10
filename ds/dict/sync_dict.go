package dict

import "sync"

type SyncDict struct {
	m sync.Map
}

func (d *SyncDict) Get(key string) (val interface{}, exists bool) {
	return d.m.Load(key)
}

func (d *SyncDict) Len() int {
	length := 0
	d.m.Range(func(key, value any) bool {
		length++
		return true
	})
	return length
}

// Put puts key value into dict (if key exists, update val and return 0, otherwise insert k-v and return 1)
func (d *SyncDict) Put(key string, val interface{}) (result int) {
	if _, exist := d.m.Load(key); exist {
		d.m.Store(key, val)
		return 0
	}
	d.m.Store(key, val)
	return 1
}

func (d *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	if _, exist := d.m.Load(key); !exist {
		d.m.Store(key, val)
		return 1
	}
	return 0
}

func (d *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	if _, exist := d.m.Load(key); exist {
		d.m.Store(key, val)
		return 1
	}
	return 0
}

func (d *SyncDict) Remove(key string) (result int) {
	if _, exist := d.m.Load(key); exist {
		d.m.Delete(key)
		return 1
	}
	return 0
}

func (d *SyncDict) ForEach(consumer Consumer) {
	d.m.Range(func(key, value any) bool {
		return consumer(key.(string), value)
	})
}

func (d *SyncDict) Keys() []string {
	keys := make([]string, d.Len())
	i := 0
	d.m.Range(func(key, value any) bool {
		keys[i] = key.(string)
		i++
		return true
	})
	return keys
}

func (d *SyncDict) RandomKeys(limit int) []string {
	keys := make([]string, d.Len())
	for i := 0; i < limit; i++ {
		d.m.Range(func(key, value any) bool {
			keys[i] = key.(string)
			return false
		})
	}
	return keys
}

func (d *SyncDict) RandomDistinctKeys(limit int) []string {
	keys := make([]string, d.Len())
	i := 0
	d.m.Range(func(key, value any) bool {
		keys[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return keys
}

func (d *SyncDict) clear() {
	*d = *MakeSyncDict()
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}
