package filter

import (
	"errors"
	"fmt"
	"path"
)

type BloomFilters struct {
	directory string
	filters   []*BloomFilter
}

type BloomFilterOptions struct {
	Capacity          int
	DataSize          int
	FileNamePrefix    string
	FalsePositiveRate float64
}

func NewBloomFilters(directory string) (*BloomFilters, error) {
	if len(directory) == 0 {
		return nil, errors.New("bloom filter is persistent and needs a directory fileName")
	}
	return &BloomFilters{directory: directory}, nil
}

func (bloomFilters *BloomFilters) NewBloomFilter(options BloomFilterOptions) (*BloomFilter, error) {
	if options.FalsePositiveRate <= 0 || options.FalsePositiveRate >= 1 {
		return nil, errors.New("bloom filter false positive rate must be between 0 and 1")
	}
	if len(options.FileNamePrefix) == 0 {
		return nil, errors.New("bloom filter needs a prefix which will be a part of its name")
	}

	fileName := path.Join(bloomFilters.directory, fmt.Sprintf("%s_%v.bloom", options.FileNamePrefix, options.Capacity))
	if filter, err := newBloomFilter(minCapacityToEnsureZeroFalseNegatives(options), options.DataSize, options.FalsePositiveRate, fileName); err != nil {
		return nil, err
	} else {
		bloomFilters.filters = append(bloomFilters.filters, filter)
		return filter, nil
	}
}

func minCapacityToEnsureZeroFalseNegatives(options BloomFilterOptions) int {
	if options.Capacity <= 10 {
		return 2 * options.Capacity
	}
	return options.Capacity
}
