package storage

import (
	"os"
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
	"testing"
)

func TestMemTableFlusherWithSuccessAsStatus(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	key := db.NewSlice([]byte("HDD"))
	value := db.NewSlice([]byte("Hard disk"))

	memTable.Put(key, value)

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	memTableFlusher := NewMemTableFlusher(memTable, directory)
	statusChannel := memTableFlusher.Flush()
	status := <-statusChannel

	if status.status != SUCCESS {
		t.Fatalf("Expected memtable flush status to be SUCCESS but received %v", status)
	}
}

func TestMemTableFlusherWithFailureAsStatus(t *testing.T) {
	emptyMemTable := NewMemTable(10, comparator.StringKeyComparator{})

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	memTableFlusher := NewMemTableFlusher(emptyMemTable, directory)
	statusChannel := memTableFlusher.Flush()
	status := <-statusChannel

	if status.status != FAILURE {
		t.Fatalf("Expected memtable flush status to be FAILURE but received %v", status.status)
	}
}
