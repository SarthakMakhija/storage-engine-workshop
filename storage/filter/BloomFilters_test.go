package filter

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db"
	"testing"
)

func tempDirectory() string {
	dir, err := ioutil.TempDir(".", "bloom")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestAddsAKeyWithBloomFilterAndChecksForItsPositiveExistence(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	bloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       1,
		FileNamePrefix: "1",
	})

	key := db.NewSlice([]byte("Company"))
	_ = bloomFilter.Put(key)

	if bloomFilter.Has(key) == false {
		t.Fatalf("Expected %v key to be present but was not", key.AsString())
	}
}

func TestAddsAKeyWithBloomFilterAndChecksForTheExistenceOfANonExistingKey(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	bloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       1,
		FileNamePrefix: "2",
	})

	key := db.NewSlice([]byte("Company"))
	_ = bloomFilter.Put(key)

	if bloomFilter.Has(db.NewSlice([]byte("Missing"))) == true {
		t.Fatalf("Expected %v key to be missing but was present", db.NewSlice([]byte("Missing")).AsString())
	}
}
