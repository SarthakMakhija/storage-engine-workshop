package storage

import (
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
	"testing"
)

func TestPutAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	memTable.Put(key, value)

	storedValue, _ := memTable.Get(key)
	if storedValue.AsString() != "Hard disk" {
		t.Fatalf("Expected %v, received %v", "Hard disk", storedValue.AsString())
	}
}

func TestPutAKeyValueAndAssertsItsExistenceInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	memTable.Put(key, value)

	_, ok := memTable.Get(key)
	if ok != true {
		t.Fatalf("Expected key to exist, but it did not. Key was %v", "HDD")
	}
}
