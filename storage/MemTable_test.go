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

	getResult := memTable.Get(key)
	if getResult.Value.AsString() != "Hard disk" {
		t.Fatalf("Expected %v, received %v", "Hard disk", getResult.Value.AsString())
	}
}

func TestPutAKeyValueAndAssertsItsExistenceInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	memTable.Put(key, value)

	getResult := memTable.Get(key)
	if getResult.Exists != true {
		t.Fatalf("Expected key to exist, but it did not. Key was %v", "HDD")
	}
}

func TestPutsKeyValuesAndDoesMultiGetByKeyInNodeInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))
	memTable.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")))

	keys := []db.Slice{
		db.NewSlice([]byte("HDD")),
		db.NewSlice([]byte("SDD")),
		db.NewSlice([]byte("PMEM")),
	}
	multiGetResult := memTable.MultiGet(keys)
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

func TestPutAKeyValueAndGetsTheAggregatePersistentSlice(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	memTable.Put(key, value)
	persistentSlice := memTable.AggregatedPersistentSlice()

	persistentKey, persistentValue := db.NewPersistentSliceKeyValuePair(persistentSlice.GetPersistentContents())
	if persistentKey.GetSlice().AsString() != key.AsString() {
		t.Fatalf("Expected key to be %v from persistent slice but received %v", key.AsString(), persistentKey.GetSlice().AsString())
	}
	if persistentValue.GetSlice().AsString() != value.AsString() {
		t.Fatalf("Expected value to be %v from persistent slice but received %v", value.AsString(), persistentValue.GetSlice().AsString())
	}
}
