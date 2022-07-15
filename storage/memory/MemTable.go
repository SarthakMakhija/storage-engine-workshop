package memory

import (
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/utils"
)

type MemTable struct {
	head           *Node
	size           uint32
	totalKeys      int
	keyComparator  comparator.KeyComparator
	levelGenerator utils.LevelGenerator
}

func NewMemTable(maxLevel int, keyComparator comparator.KeyComparator) *MemTable {
	return &MemTable{
		head:           NewNode(db.NilSlice(), db.NilSlice(), maxLevel),
		size:           0,
		keyComparator:  keyComparator,
		levelGenerator: utils.NewLevelGenerator(maxLevel),
	}
}

func (memTable *MemTable) Put(key, value db.Slice) bool {
	if ok := memTable.head.Put(key, value, memTable.keyComparator, memTable.levelGenerator); ok {
		memTable.size = memTable.size + uint32(key.Size()) + uint32(value.Size())
		memTable.totalKeys = memTable.totalKeys + 1
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

func (memTable *MemTable) AllKeyValues() []db.KeyValuePair {
	return memTable.head.AllKeyValues()
}

func (memTable *MemTable) TotalSize() uint32 {
	return memTable.size
}

func (memTable *MemTable) TotalKeys() int {
	return memTable.totalKeys
}
