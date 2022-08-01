package db

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"strconv"
	"testing"
	"time"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "db")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestPut1000KeysValuesAndGetByKeys(t *testing.T) {
	const segmentMaxSizeBytes uint64 = 10 * 1024 * 1024
	const bufferMaxSizeBytes uint64 = 512

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	keyUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	db, _ := NewKeyValueDb(configuration)

	txn := db.newTransaction()
	for count := 1; count <= 1000; count++ {
		_ = txn.Put(keyUsing(count), valueUsing(count))
	}
	if err := txn.Commit(); err != nil {
		log.Fatal(err)
	}

	allowFlushingSSTable()

	readonlyTxn := db.newReadonlyTransaction()
	for count := 1; count <= 1000; count++ {
		getResult := readonlyTxn.Get(keyUsing(count))
		expectedValue := valueUsing(count)

		if getResult.Value.AsString() != expectedValue.AsString() {
			t.Fatalf("Expected %v, received %v", expectedValue.AsString(), getResult.Value.AsString())
		}
	}
}

func allowFlushingSSTable() {
	time.Sleep(1 * time.Second)
}
