package log

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db"
	"strconv"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "wal")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestAppendsToWriteAheadLogAndReadsTheEntireLog(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	var segmentMaxSizeBytes uint64 = 32
	wal, _ := NewLog(directory, segmentMaxSizeBytes)
	for count := 1; count <= 20; count++ {
		putCommand := NewPutCommand(keyUsing(count), valueUsing(count))
		err := wal.Append(putCommand)
		if err != nil {
			log.Fatal(err)
		}
	}

	putCommands, err := wal.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	for count := 1; count <= 20; count++ {
		expectedKey := keyUsing(count)
		expectedValue := valueUsing(count)
		putCommand := putCommands[count-1]

		if putCommand.key.AsString() != expectedKey.AsString() {
			t.Fatalf("Expected key %v, received %v", expectedKey.AsString(), putCommand.key.AsString())
		}
		if putCommand.value.AsString() != expectedValue.AsString() {
			t.Fatalf("Expected value %v, received %v", expectedValue.AsString(), putCommand.value.AsString())
		}
	}
}
