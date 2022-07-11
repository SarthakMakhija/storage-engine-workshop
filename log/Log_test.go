package log

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "wal")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestAppendsToWriteAheadLogAndReadsTheKey(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	wal, _ := NewLog(directory)
	putCommand := NewPutCommand(db.NewSlice([]byte("Company")), db.NewSlice([]byte("TW")))
	_ = wal.Append(putCommand)

	readPutCommand, _ := wal.ReadAt(0)
	if readPutCommand.key.AsString() != "Company" {
		t.Fatalf("Expected Key to be %v, received %v", "Company", readPutCommand.key.AsString())
	}
}

func TestAppendsToWriteAheadLogAndReadsTheValue(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	wal, _ := NewLog(directory)
	putCommand := NewPutCommand(db.NewSlice([]byte("Company")), db.NewSlice([]byte("TW")))
	_ = wal.Append(putCommand)

	readPutCommand, _ := wal.ReadAt(0)
	if readPutCommand.value.AsString() != "TW" {
		t.Fatalf("Expected Key to be %v, received %v", "TW", readPutCommand.value.AsString())
	}
}
