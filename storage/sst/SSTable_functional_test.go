package sst

import (
	"os"
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/memory"
	"strconv"
	"testing"
)

func TestCreatesSSTableWith500KeysAndPutsAllKeysInBloomFilter(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})
	keyUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}
	for count := 1; count <= 500; count++ {
		memTable.Put(keyUsing(count), valueUsing(count))
	}

	ssTable, _ := NewSSTableFrom(memTable, directory)
	for count := 1; count <= 500; count++ {
		key := keyUsing(count)
		contains := ssTable.bloomFilter.Has(key)

		if contains == false {
			t.Fatalf("Expected key %v to be present in bloom filter corresponding to the SSTable but was not",
				key.AsString(),
			)
		}
	}
}
