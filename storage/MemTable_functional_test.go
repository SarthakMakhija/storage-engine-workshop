package storage

import (
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
	"strconv"
	"testing"
)

func TestPut500KeysValuesAndGetByKeys(t *testing.T) {
	memTable := NewMemTable(10, comparator.StringKeyComparator{})

	keyUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	valueUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Value-" + strconv.Itoa(count)))
	}

	for count := 1; count <= 500; count++ {
		memTable.Put(keyUsing(count), valueUsing(count))
	}

	for count := 1; count <= 500; count++ {
		storedValue, _ := memTable.Get(keyUsing(count))
		expectedValue := valueUsing(count)

		if storedValue.AsString() != expectedValue.AsString() {
			t.Fatalf("Expected %v, received %v", expectedValue.AsString(), storedValue.AsString())
		}
	}
}
