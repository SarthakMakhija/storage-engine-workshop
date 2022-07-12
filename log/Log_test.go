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

	const pageSize = 100
	wal, _ := NewLog(directory, pageSize)
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

	const pageSize = 100
	wal, _ := NewLog(directory, pageSize)
	putCommand := NewPutCommand(db.NewSlice([]byte("Company")), db.NewSlice([]byte("TW")))
	_ = wal.Append(putCommand)

	readPutCommand, _ := wal.ReadAt(0)
	if readPutCommand.value.AsString() != "TW" {
		t.Fatalf("Expected Key to be %v, received %v", "TW", readPutCommand.value.AsString())
	}
}

func TestAppendsToWriteAheadLogWithACommandThatExceedsThePageRemainingCapacityAndReadsTheValueAtFirstOffset(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	const pageSize = 20
	wal, _ := NewLog(directory, pageSize)
	companyCommand := NewPutCommand(db.NewSlice([]byte("Company")), db.NewSlice([]byte("TW")))
	_ = wal.Append(companyCommand)
	diskCommand := NewPutCommand(db.NewSlice([]byte("Disk")), db.NewSlice([]byte("HDD")))
	_ = wal.Append(diskCommand)

	readPutCommand, _ := wal.ReadAt(0)
	if readPutCommand.value.AsString() != "TW" {
		t.Fatalf("Expected Key to be %v, received %v", "TW", readPutCommand.value.AsString())
	}
}

func TestAppendsToWriteAheadLogWithACommandThatExceedsThePageRemainingCapacityAndReadsTheValueAtNextAvailableOffset(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	const pageSize = 20
	wal, _ := NewLog(directory, pageSize)
	companyCommand := NewPutCommand(db.NewSlice([]byte("Company")), db.NewSlice([]byte("TW")))
	_ = wal.Append(companyCommand)
	diskCommand := NewPutCommand(db.NewSlice([]byte("Disk")), db.NewSlice([]byte("HDD")))
	_ = wal.Append(diskCommand)

	readPutCommand, _ := wal.ReadAt(pageSize)
	if readPutCommand.value.AsString() != "HDD" {
		t.Fatalf("Expected Key to be %v, received %v", "HDD", readPutCommand.value.AsString())
	}
}
