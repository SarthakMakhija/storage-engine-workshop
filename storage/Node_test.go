package storage

import (
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
	"storage-engine-workshop/utils"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(db.NilSlice(), db.NilSlice(), maxLevel)

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value, keyComparator, utils.NewLevelGenerator(maxLevel))

	getResult := sentinelNode.Get(key, keyComparator)
	if getResult.Value.AsString() != "Hard disk" {
		t.Fatalf("Expected %v, received %v", "Hard disk", getResult.Value.AsString())
	}
}

func TestPutAKeyValueAndAssertsItsExistenceInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(db.NilSlice(), db.NilSlice(), maxLevel)

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value, keyComparator, utils.NewLevelGenerator(maxLevel))

	getResult := sentinelNode.Get(key, keyComparator)
	if getResult.Exists != true {
		t.Fatalf("Expected key to exist, but it did not. Key was %v", "HDD")
	}
}

func TestPutsKeyValuesAndDoesMultiGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(db.NilSlice(), db.NilSlice(), maxLevel)

	sentinelNode.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")), keyComparator, utils.NewLevelGenerator(maxLevel))
	sentinelNode.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")), keyComparator, utils.NewLevelGenerator(maxLevel))

	keys := []db.Slice{
		db.NewSlice([]byte("HDD")),
		db.NewSlice([]byte("SDD")),
		db.NewSlice([]byte("PMEM")),
	}
	multiGetResult := sentinelNode.MultiGet(keys, keyComparator)
	allGetResults := multiGetResult.Values

	expected := []db.GetResult{
		{Value: db.NewSlice([]byte("Hard disk")), Exists: true},
		{Value: db.NilSlice(), Exists: false},
		{Value: db.NewSlice([]byte("Solid state")), Exists: true},
	}

	for index, e := range expected {
		if e.Value.AsString() != allGetResults[index].Value.AsString() {
			t.Fatalf("Expected %v, received %v", e.Value.AsString(), allGetResults[index].Value.AsString())
		}
	}
}
