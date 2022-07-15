package memory

import (
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/comparator"
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

	keyValuePairs := memTable.AllKeyValues()

	if keyValuePairs[0].Key.AsString() != key.AsString() {
		t.Fatalf("Expected key to be %v from all keys but received %v", key.AsString(), keyValuePairs[0].Key.AsString())
	}
	if keyValuePairs[0].Value.AsString() != value.AsString() {
		t.Fatalf("Expected value to be %v from all keys but received %v", value.AsString(), keyValuePairs[0].Value.AsString())
	}
}

func TestPutAKeyValueAndGetsTheTotalKeysInMemTable(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))
	memTable.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")))

	totalKeys := memTable.TotalKeys()

	if totalKeys != 2 {
		t.Fatalf("Expected %v keys but received %v", 2, totalKeys)
	}
}

func TestReturnsTheTotalMemTableSize(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})
	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))
	memTable.Put(key, value)

	size := memTable.TotalSize()
	expected := key.Size() + value.Size()

	if size != uint32(expected) {
		t.Fatalf("Expected total memtable size to be %v, received %v", expected, size)
	}
}
