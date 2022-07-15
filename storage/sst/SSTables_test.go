package sst

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/memory"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "sst")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestWritesSSTableToDisk(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	if err := ssTable.Write(); err != nil {
		t.Fatalf("Expected no errors while dump sstable file but received an error: %v", err)
	}
}

func TestWrites2SSTablesToDisk(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTableA, _ := ssTables.NewSSTable(memTable)
	ssTableB, _ := ssTables.NewSSTable(memTable)

	if err := ssTableA.Write(); err != nil {
		t.Fatalf("Expected no errors while dump sstable file but received an error: %v", err)
	}
	if err := ssTableB.Write(); err != nil {
		t.Fatalf("Expected no errors while dump sstable file but received an error: %v", err)
	}
}

func TestCreatesSSTableAndPutsKeysInBloomFilter(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))
	memTable.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	contains := ssTable.bloomFilter.Has(db.NewSlice([]byte("SDD")))

	if contains == false {
		t.Fatalf("Expected key %v to be present in bloom filter corresponding to the SSTable but was not",
			db.NewSlice([]byte("SDD")).AsString(),
		)
	}
}

func TestGetsFromSSTable(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	getResult := ssTable.Get(db.NewSlice([]byte("HDD")), comparator.StringKeyComparator{})
	if getResult.Value.AsString() != "Hard disk" {
		t.Fatalf("Expected value to be %v, received %v", "Hard disk", getResult.Value.AsString())
	}
}

func TestGetsFromSSTableContainingMultipleKeyValues(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))
	memTable.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")))
	memTable.Put(db.NewSlice([]byte("Pmem")), db.NewSlice([]byte("Persistent memory")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	getResult := ssTable.Get(db.NewSlice([]byte("SDD")), comparator.StringKeyComparator{})
	if getResult.Value.AsString() != "Solid state" {
		t.Fatalf("Expected value to be %v, received %v", "Solid state", getResult.Value.AsString())
	}
}

func TestGetNonExistentKeyFromSSTableContainingMultipleKeyValues(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))
	memTable.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")))
	memTable.Put(db.NewSlice([]byte("Pmem")), db.NewSlice([]byte("Persistent memory")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTables, _ := NewSSTables(directory)
	ssTable, _ := ssTables.NewSSTable(memTable)
	_ = ssTable.Write()

	getResult := ssTable.Get(db.NewSlice([]byte("Unknown")), comparator.StringKeyComparator{})
	if getResult.Exists != false {
		t.Fatalf("Expected value to be missing for key %v, but was present", "Unknown")
	}
}

func TestGetsFromSSTablesBasedOnBloomFilter(t *testing.T) {
	directory := tempDirectory()
	ssTables, _ := NewSSTables(directory)
	defer os.RemoveAll(directory)

	memTableA := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTableA.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))
	memTableA.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")))

	ssTableA, _ := ssTables.NewSSTable(memTableA)
	_ = ssTableA.Write()

	memTableB := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTableB.Put(db.NewSlice([]byte("PMEM")), db.NewSlice([]byte("Persistent memory")))
	memTableB.Put(db.NewSlice([]byte("NVMe")), db.NewSlice([]byte("Non volatile media")))

	ssTableB, _ := ssTables.NewSSTable(memTableB)
	_ = ssTableB.Write()

	requestedKeyValuePairs := []db.KeyValuePair{
		{db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk"))},
		{db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state"))},
		{db.NewSlice([]byte("PMEM")), db.NewSlice([]byte("Persistent memory"))},
		{db.NewSlice([]byte("NVMe")), db.NewSlice([]byte("Non volatile media"))},
	}
	for _, pair := range requestedKeyValuePairs {
		getResult := ssTables.Get(pair.Key, comparator.StringKeyComparator{})
		if getResult.Value.AsString() != pair.Value.AsString() {
			t.Fatalf("Expected value to be %v, received %v", pair.Value.AsString(), getResult.Value.AsString())
		}
	}
}
