package log

import (
	"os"
	"path"
	"storage-engine-workshop/db"
)

type Store struct {
	file          *os.File
	currentOffset int64
	pageSize      int
}

func NewStore(directory string, pageSize int) (*Store, error) {
	storeFile, err := os.OpenFile(path.Join(directory, "1.store"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{file: storeFile, currentOffset: 0, pageSize: pageSize}, nil
}

func (store *Store) Append(persistentSlice db.PersistentSlice) error {
	writeAt := func(offset int64) error {
		bytesWritten, err := store.file.WriteAt(persistentSlice.GetPersistentContents(), offset)
		if err != nil {
			return err
		}
		store.currentOffset = store.currentOffset + int64(bytesWritten)
		return nil
	}
	availableCapacityInPage := func() int64 {
		currentPage := store.currentOffset / int64(store.pageSize)
		currentPageBeginOffset := currentPage * int64(store.pageSize)
		return int64(store.pageSize) - (store.currentOffset - currentPageBeginOffset)
	}
	newOffset := func() int64 {
		currentPage := store.currentOffset / int64(store.pageSize)
		nextPage := currentPage + 1
		return nextPage * int64(store.pageSize)
	}

	if int64(persistentSlice.Size()) <= availableCapacityInPage() {
		return writeAt(store.currentOffset)
	}
	return writeAt(newOffset())
}

func (store *Store) ReadAt(offset int64) (db.PersistentSlice, db.PersistentSlice, error) {
	bytes := make([]byte, int(db.ReservedTotalSize))
	_, err := store.file.ReadAt(bytes, offset)
	if err != nil {
		return db.NilPersistentSlice(), db.NilPersistentSlice(), err
	}
	sizeToRead := db.ActualTotalSize(bytes)
	contents := make([]byte, sizeToRead)

	_, err = store.file.ReadAt(contents, offset)
	if err != nil {
		return db.NilPersistentSlice(), db.NilPersistentSlice(), err
	}
	key, value := db.NewPersistentSliceKeyValuePair(contents)
	return key, value, nil
}
