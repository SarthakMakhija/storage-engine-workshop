package filter

import (
	"os"
	"storage-engine-workshop/db"
	"testing"
)

func TestAddsAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	defer os.RemoveAll("./bloom.filter")

	bloomFilter, _ := NewBloomFilter(
		BloomFilterOptions{
			Path:              "./bloom.filter",
			FalsePositiveRate: 0.001,
			Capacity:          500,
		},
	)
	key := db.NewSlice([]byte("Company"))
	_ = bloomFilter.Put(key)

	if bloomFilter.Has(key) == false {
		t.Fatalf("Expected %v key to be present but was not", key.AsString())
	}
}

func TestAddsAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	defer os.RemoveAll("./bloom.filter")

	bloomFilter, _ := NewBloomFilter(
		BloomFilterOptions{
			Path:              "./bloom.filter",
			FalsePositiveRate: 0.001,
			Capacity:          500,
		},
	)
	key := db.NewSlice([]byte("Company"))
	_ = bloomFilter.Put(key)

	if bloomFilter.Has(db.NewSlice([]byte("Missing"))) == true {
		t.Fatalf("Expected %v key to be missing but was present", db.NewSlice([]byte("Missing")).AsString())
	}
}
