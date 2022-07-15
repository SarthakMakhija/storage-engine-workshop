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

	ssTable, _ := NewSSTableFrom(memTable, directory)
	if err := ssTable.Write(); err != nil {
		t.Fatalf("Expected no errors while dump sstable file but received an error: %v", err)
	}
}

func TestCreatesSSTableAndPutsKeysInBloomFilter(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	memTable.Put(db.NewSlice([]byte("HDD")), db.NewSlice([]byte("Hard disk")))
	memTable.Put(db.NewSlice([]byte("SDD")), db.NewSlice([]byte("Solid state")))

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	ssTable, _ := NewSSTableFrom(memTable, directory)
	_ = ssTable.Write()

	contains := ssTable.bloomFilter.Has(db.NewSlice([]byte("SDD")))

	if contains == false {
		t.Fatalf("Expected key %v to be present in bloom filter corresponding to the SSTable but was not",
			db.NewSlice([]byte("SDD")).AsString(),
		)
	}
}
