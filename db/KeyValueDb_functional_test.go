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

	keyUsing := func(id, count int) model.Slice {
		return model.NewSlice([]byte("Key-" + strconv.Itoa(id) + "-" + strconv.Itoa(count)))
	}
	valueUsing := func(id, count int) model.Slice {
		return model.NewSlice([]byte("Value-" + strconv.Itoa(id) + "-" + strconv.Itoa(count)))
	}

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	db, _ := NewKeyValueDb(configuration)

	txns := []*Transaction{db.newTransaction(), db.newTransaction(), db.newTransaction(), db.newTransaction(), db.newTransaction()}
	for transactionId := 0; transactionId < 5; transactionId++ {
		for count := 1; count <= 200; count++ {
			_ = txns[transactionId].Put(keyUsing(transactionId, count), valueUsing(transactionId, count))
		}
		err := txns[transactionId].Commit()
		if err != nil {
			log.Fatal(err)
		}
	}

	allowFlushingSSTable()

	readonlyTxn := db.newReadonlyTransaction()
	for transactionId := 0; transactionId < 5; transactionId++ {
		for count := 1; count <= 200; count++ {
			getResult := readonlyTxn.Get(keyUsing(transactionId, count))
			expectedValue := valueUsing(transactionId, count)

			if getResult.Value.AsString() != expectedValue.AsString() {
				t.Fatalf("Expected %v, received %v", expectedValue.AsString(), getResult.Value.AsString())
			}
		}
	}
}

func allowFlushingSSTable() {
	time.Sleep(1 * time.Second)
}
