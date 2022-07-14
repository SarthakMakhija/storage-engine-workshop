package filter

import (
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
	"math"
	"storage-engine-workshop/db"
	"unsafe"
)

const byteSize = int(unsafe.Sizeof(byte(0)))

type BloomFilter struct {
	falsePositiveRate     float64
	capacity              int
	bitVectorSize         int
	store                 *Store
	bitsPerHashFunction   int
	numberOfHashFunctions int
	seeds                 []uint32
	path                  string
	dataSize              int
}

type BloomFilterOptions struct {
	Path              string
	FalsePositiveRate float64
	Capacity          int
	DataSize          int
}

func NewBloomFilter(options BloomFilterOptions) (*BloomFilter, error) {
	if options.FalsePositiveRate <= 0 || options.FalsePositiveRate >= 1 {
		return nil, errors.New("bloom filter false positive rate must be between 0 and 1")
	}
	if options.Capacity <= 10 {
		return nil, errors.New("bloom filter capacity must be greater than 10")
	}
	if len(options.Path) == 0 {
		return nil, errors.New("bloom filter is persistent and needs a file path")
	}
	numberOfHashFunctions := numberOfHashFunctions(options.FalsePositiveRate)
	bitVectorSize, bitsPerHashFunction := bitVector(options.Capacity, options.FalsePositiveRate, numberOfHashFunctions)
	bitVectorSize = bitVectorSize / byteSize
	bitVectorSize = bitVectorSize + byteSize

	store, err := NewStore(options.Path, options.DataSize+bitVectorSize)
	if err != nil {
		return nil, err
	}
	return &BloomFilter{
		falsePositiveRate:     options.FalsePositiveRate,
		capacity:              options.Capacity,
		bitVectorSize:         bitVectorSize,
		bitsPerHashFunction:   bitsPerHashFunction,
		seeds:                 hashSeeds(numberOfHashFunctions),
		numberOfHashFunctions: numberOfHashFunctions,
		path:                  options.Path,
		dataSize:              options.DataSize + bitVectorSize,
		store:                 store,
	}, nil
}

func (bloomFilter *BloomFilter) Put(key db.Slice) error {
	indices := bloomFilter.keyIndices(key)

	for index := 0; index < len(indices); index++ {
		bytePosition, mask := bloomFilter.bitPositionInByte(indices[index])
		if int(bytePosition) >= bloomFilter.store.Size() {
			return errors.New(fmt.Sprintf("bytePosition %v is greater than bloom filter file size for indices[index] %v", bytePosition, indices[index]))
		}
		bloomFilter.store.SetBit(bytePosition, mask)
	}
	return nil
}

func (bloomFilter *BloomFilter) Has(key db.Slice) bool {
	indices := bloomFilter.keyIndices(key)

	for index := 0; index < len(indices); index++ {
		bytePosition, mask := bloomFilter.bitPositionInByte(indices[index])
		if int(bytePosition) >= bloomFilter.store.Size() {
			return false
		}
		if bloomFilter.store.GetBit(bytePosition)&mask == 0 {
			return false
		}
	}
	return true
}

func (bloomFilter *BloomFilter) bitPositionInByte(keyIndex uint64) (uint64, byte) {
	quotient, remainder := int64(keyIndex)/int64(byteSize), int64(keyIndex)%int64(byteSize)
	maxPossibleValueWithByte := int64(math.Pow(2, float64(byteSize)-1)) //128
	if remainder == 0 {
		return uint64(quotient), byte(maxPossibleValueWithByte)
	}
	return uint64(quotient), byte(maxPossibleValueWithByte >> (remainder - 1))
}

// Use the hash function to get all keyIndices of the given key
func (bloomFilter *BloomFilter) keyIndices(key db.Slice) []uint64 {
	indices := make([]uint64, 0, len(bloomFilter.seeds))
	runHash := func(key []byte, seed uint32) uint64 {
		hash, _ := murmur3.Sum128WithSeed(key, seed)
		return hash
	}
	indexForHash := func(hash uint64, index int) uint64 {
		return uint64(index*bloomFilter.bitsPerHashFunction) + (hash % uint64(bloomFilter.bitsPerHashFunction))
	}
	for index, seed := range bloomFilter.seeds {
		hash := runHash(key.GetRawContent(), seed)
		indices = append(indices, indexForHash(hash, index))
	}
	return indices
}

//Calculate K
func numberOfHashFunctions(falsePositiveRate float64) int {
	return int(math.Ceil(math.Log2(1.0 / falsePositiveRate)))
}

//Calculate bitVectorSize(M) and bitsPerHashFunction(m)
func bitVector(capacity int, falsePositiveRate float64, numberOfHashFunctions int) (int, int) {
	ln2RaiseTo2 := math.Pow(math.Ln2, 2)
	bitVectorSize := int(float64(capacity) * math.Abs(math.Log(falsePositiveRate)) / ln2RaiseTo2)
	bitsPerHashFunction := bitVectorSize / numberOfHashFunctions

	return bitVectorSize, bitsPerHashFunction
}

//Compute seed values for hash function(s)
func hashSeeds(numberOfHashFunctions int) []uint32 {
	seeds := make([]uint32, numberOfHashFunctions)
	for index := 0; index < len(seeds); index++ {
		seeds[index] = 32 << int64(index+1)
	}
	return seeds
}
