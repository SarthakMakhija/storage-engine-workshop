package filter

import (
	"io/ioutil"
	"log"
	"os"
	"storage-engine-workshop/db"
	"strconv"
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

func TestAddsAKeyWithBloomFilterAndChecksForItsPositiveExistenceSimulatingARestart(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	aBloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       1,
		FileNamePrefix: "1",
	})

	_ = aBloomFilter.Put(db.NewSlice([]byte("Company")))
	_ = aBloomFilter.Put(db.NewSlice([]byte("State")))

	bloomFilters.Close()
	bloomFiltersAfterRestart, _ := NewBloomFilters(directory, 0.001)

	if bloomFiltersAfterRestart.Has(db.NewSlice([]byte("Company"))) == false {
		t.Fatalf("Expected key %v to be present but was not", db.NewSlice([]byte("Company")).AsString())
	}
	if bloomFiltersAfterRestart.Has(db.NewSlice([]byte("State"))) == false {
		t.Fatalf("Expected key %v to be present but was not", db.NewSlice([]byte("State")).AsString())
	}
}

func TestAddsAKeyWithMultipleBloomFiltersAndChecksForItsPositiveExistenceSimulatingARestart(t *testing.T) {
	directory := tempDirectory()
	defer os.RemoveAll(directory)

	bloomFilters, _ := NewBloomFilters(directory, 0.001)
	aBloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       2,
		FileNamePrefix: "1",
	})
	_ = aBloomFilter.Put(db.NewSlice([]byte("Key-1")))
	_ = aBloomFilter.Put(db.NewSlice([]byte("Key-2")))

	bBloomFilter, _ := bloomFilters.NewBloomFilter(BloomFilterOptions{
		Capacity:       2,
		FileNamePrefix: "1",
	})
	_ = bBloomFilter.Put(db.NewSlice([]byte("Key-3")))
	_ = bBloomFilter.Put(db.NewSlice([]byte("Key-4")))

	bloomFilters.Close()
	bloomFiltersAfterRestart, _ := NewBloomFilters(directory, 0.001)

	for count := 1; count <= 4; count++ {
		key := db.NewSlice([]byte("Key-" + strconv.Itoa(count)))
		contains := bloomFiltersAfterRestart.Has(key)
		if contains == false {
			t.Fatalf("Expected key %v to be present but was not", key.AsString())
		}
	}
}
