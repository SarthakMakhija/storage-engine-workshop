package filter

import (
	"os"
	"storage-engine-workshop/db"
	"strconv"
	"testing"
)

func TestAdds500KeysAndChecksForTheirPositiveExistence(t *testing.T) {
	defer os.RemoveAll("./bloom.filter")

	bloomFilter, _ := NewBloomFilter(
		BloomFilterOptions{
			Path:              "./bloom.filter",
			FalsePositiveRate: 0.001,
			Capacity:          500,
		},
	)

	keyUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	for count := 1; count <= 500; count++ {
		_ = bloomFilter.Put(keyUsing(count))
	}

	for count := 1; count <= 500; count++ {
		contains := bloomFilter.Has(keyUsing(count))
		if contains == false {
			t.Fatalf("Expected key %v to be present but was not", keyUsing(count).AsString())
		}
	}
}

func TestAdds500KeysAndChecksForTheExistenceOfMissingKeys(t *testing.T) {
	defer os.RemoveAll("./bloom.filter")

	bloomFilter, _ := NewBloomFilter(
		BloomFilterOptions{
			Path:              "./bloom.filter",
			FalsePositiveRate: 0.001,
			Capacity:          500,
		},
	)

	keyUsing := func(count int) db.Slice {
		return db.NewSlice([]byte("Key-" + strconv.Itoa(count)))
	}
	for count := 1; count <= 500; count++ {
		_ = bloomFilter.Put(keyUsing(count))
	}

	for count := 1; count <= 500; count++ {
		contains := bloomFilter.Has(keyUsing(count * 600))
		if contains == true {
			t.Fatalf("Expected key %v to be missing but was present", keyUsing(count*600).AsString())
		}
	}
}