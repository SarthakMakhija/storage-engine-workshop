package sst

import (
	"errors"
	"fmt"
	"path"
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/filter"
	"storage-engine-workshop/storage/memory"
	"strconv"
)

type SSTable struct {
	totalKeys     int
	store         *Store
	keyValuePairs []db.KeyValuePair
	bloomFilter   *filter.BloomFilter
}

func NewSSTableFrom(memTable *memory.MemTable, bloomFilters *filter.BloomFilters, directory string, fileId int) (*SSTable, error) {
	store, err := NewStore(path.Join(directory, fmt.Sprintf("%v.sst", fileId)))
	if err != nil {
		return nil, err
	}
	bloomFilter, err := createBloomFilter(fileId, memTable.TotalKeys(), bloomFilters)
	if err != nil {
		return nil, err
	}
	return &SSTable{
		totalKeys:     memTable.TotalKeys(),
		store:         store,
		keyValuePairs: memTable.AllKeyValues(),
		bloomFilter:   bloomFilter,
	}, nil
}

func (ssTable *SSTable) Write() error {
	if len(ssTable.keyValuePairs) == 0 {
		return errors.New("ssTable does not contain any key value pairs to write to " + ssTable.store.file.Name())
	}
	beginOffsetByKey, offset, err := ssTable.writeKeyValues()
	if err != nil {
		return err
	}
	indexBlock := NewIndexBlock(ssTable.store)
	if err := indexBlock.Write(beginOffsetByKey, offset, ssTable.keyValuePairs); err != nil {
		return err
	}
	if err := ssTable.store.Sync(); err != nil {
		return errors.New("error while syncing the ssTable file " + ssTable.store.file.Name())
	}
	return nil
}

func (ssTable *SSTable) Get(key db.Slice, keyComparator comparator.KeyComparator) db.GetResult {
	indexBlock := NewIndexBlock(ssTable.store)
	keyOffset, err := indexBlock.GetKeyOffset(key, keyComparator)
	if err != nil {
		return db.GetResult{Exists: false}
	}
	if keyOffset == -1 {
		return db.GetResult{Exists: false}
	}
	_, resultValue, err := ssTable.readAt(keyOffset)
	if err != nil {
		return db.GetResult{Exists: false}
	}
	return db.GetResult{Value: resultValue.GetSlice(), Exists: true}
}

func (ssTable *SSTable) readAt(offset int64) (PersistentSSTableSlice, PersistentSSTableSlice, error) {
	bytes := make([]byte, int(reservedTotalSize))
	_, err := ssTable.store.ReadAt(bytes, offset)
	if err != nil {
		return EmptyPersistentSSTableSlice(), EmptyPersistentSSTableSlice(), err
	}
	sizeToRead := ActualTotalSize(bytes)
	contents := make([]byte, sizeToRead)

	_, err = ssTable.store.ReadAt(contents, offset)
	if err != nil {
		return EmptyPersistentSSTableSlice(), EmptyPersistentSSTableSlice(), err
	}
	key, value := NewPersistentSSTableSliceKeyValuePair(contents)
	return key, value, nil
}

func (ssTable *SSTable) writeKeyValues() ([]int64, int64, error) {
	var offset int64 = 0
	beginOffsetByKey := make([]int64, len(ssTable.keyValuePairs))

	for index, keyValuePair := range ssTable.keyValuePairs {
		if bytesWritten, err := ssTable.store.WriteAt(NewPersistentSSTableSlice(keyValuePair).GetPersistentContents(), offset); err != nil {
			return nil, 0, err
		} else {
			beginOffsetByKey[index] = offset
			offset = offset + int64(bytesWritten)
		}
		if err := ssTable.bloomFilter.Put(keyValuePair.Key); err != nil {
			return nil, 0, err
		}
	}
	return beginOffsetByKey, offset, nil
}

func createBloomFilter(fileNamePrefix int, totalKeys int, bloomFilters *filter.BloomFilters) (*filter.BloomFilter, error) {
	bloomFilter, err := bloomFilters.NewBloomFilter(filter.BloomFilterOptions{
		Capacity:       totalKeys,
		FileNamePrefix: strconv.Itoa(fileNamePrefix),
	})
	if err != nil {
		return nil, err
	}
	return bloomFilter, nil
}
