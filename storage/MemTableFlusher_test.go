package storage

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
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

func TestMemTableFlusherWithSuccessAsStatus(t *testing.T) {
	memTable := memory.NewMemTable(10, comparator.StringKeyComparator{})

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
	emptyMemTable := memory.NewMemTable(10, comparator.StringKeyComparator{})

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	memTableFlusher := NewMemTableFlusher(emptyMemTable, directory)
	statusChannel := memTableFlusher.Flush()
	status := <-statusChannel

	if status.status != FAILURE {
		t.Fatalf("Expected memtable flush status to be FAILURE but received %v", status.status)
	}
}
