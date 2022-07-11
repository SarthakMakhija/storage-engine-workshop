package storage

import (
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
	"storage-engine-workshop/utils"
	"testing"
)

func TestPutAKeyValueAndGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(db.NilSlice(), db.NilSlice(), maxLevel)

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value, keyComparator, utils.NewLevelGenerator(maxLevel))

	storedValue, _ := sentinelNode.Get(key, keyComparator)
	if storedValue.AsString() != "Hard disk" {
		t.Fatalf("Expected %v, received %v", "Hard disk", storedValue.AsString())
	}
}

func TestPutAKeyValueAndAssertsItsExistenceInNode(t *testing.T) {
	const maxLevel = 8
	keyComparator := comparator.StringKeyComparator{}

	sentinelNode := NewNode(db.NilSlice(), db.NilSlice(), maxLevel)

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	sentinelNode.Put(key, value, keyComparator, utils.NewLevelGenerator(maxLevel))

	_, ok := sentinelNode.Get(key, keyComparator)
	if ok != true {
		t.Fatalf("Expected key to exist, but it did not. Key was %v", "HDD")
	}
}
