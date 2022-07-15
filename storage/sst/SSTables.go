package sst

import (
	"errors"
	"os"
	"path"
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/comparator"
	"storage-engine-workshop/storage/filter"
	"storage-engine-workshop/storage/memory"
)

const subDirectoryPermission = 0744

type SSTables struct {
	directory    string
	nextFileId   int
	tables       []*SSTable
	bloomFilters *filter.BloomFilters
}

func NewSSTables(directory string) (*SSTables, error) {
	if len(directory) == 0 {
		return nil, errors.New("directory can not be empty while creating SSTables")
	}
	subDirectory := path.Join(directory, "sst")
	if _, err := os.Stat(subDirectory); os.IsNotExist(err) {
		if err := os.Mkdir(subDirectory, subDirectoryPermission); err != nil {
			return nil, err
		}
	}
	bloomFilters, err := filter.NewBloomFilters(directory, 0.001)
	if err != nil {
		return nil, err
	}
	return &SSTables{
		directory:    subDirectory,
		bloomFilters: bloomFilters,
		nextFileId:   1,
	}, nil
}

func (ssTables *SSTables) NewSSTable(memTable *memory.MemTable) (*SSTable, error) {
	ssTable, err := NewSSTableFrom(memTable, ssTables.bloomFilters, ssTables.directory, ssTables.nextFileId)
	if err != nil {
		return nil, err
	}
	ssTables.tables = append(ssTables.tables, ssTable)
	ssTables.nextFileId = ssTables.nextFileId + 1
	return ssTable, nil
}

func (ssTables SSTables) Get(key db.Slice, keyComparator comparator.KeyComparator) db.GetResult {
	for index := len(ssTables.tables) - 1; index >= 0; index-- {
		table := ssTables.tables[index]
		if table.bloomFilter.Has(key) {
			if getResult := table.Get(key, keyComparator); getResult.Exists {
				return getResult
			}
		}
	}
	return db.GetResult{Exists: false}
}
