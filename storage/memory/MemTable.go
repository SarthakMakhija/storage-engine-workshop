package memory

import (
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
	"storage-engine-workshop/utils"
)

type MemTable struct {
	head           *Node
	size           uint32
	keyComparator  comparator.KeyComparator
	levelGenerator utils.LevelGenerator
}

func NewMemTable(maxLevel int, keyComparator comparator.KeyComparator) *MemTable {
	return &MemTable{
		head:           NewNode(db.NilSlice(), db.NilSlice(), maxLevel),
		keyComparator:  keyComparator,
		levelGenerator: utils.NewLevelGenerator(maxLevel),
	}
}

func (memTable *MemTable) Put(key, value db.Slice) bool {
	if ok := memTable.head.Put(key, value, memTable.keyComparator, memTable.levelGenerator); ok {
		memTable.size = memTable.size + uint32(key.Size()) + uint32(value.Size())
		return ok
	}
	return false
}

func (memTable *MemTable) Get(key db.Slice) db.GetResult {
	return memTable.head.Get(key, memTable.keyComparator)
}

func (memTable *MemTable) MultiGet(keys []db.Slice) db.MultiGetResult {
	return memTable.head.MultiGet(keys, memTable.keyComparator)
}

func (memTable *MemTable) AggregatePersistentSlice() db.PersistentSlice {
	return memTable.head.AggregatePersistentSlice()
}

func (memTable *MemTable) TotalSize() uint32 {
	return memTable.size
}
