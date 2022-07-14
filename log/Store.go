package log

import (
	"errors"
	"fmt"
	"log"
	"os"
	"storage-engine-workshop/db"
)

type Store struct {
	file *os.File
	size uint64
}

func NewStore(filePath string) (*Store, error) {
	storeFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{file: storeFile, size: 0}, nil
}

func (store *Store) Append(persistentSlice db.PersistentSlice) error {
	bytesWritten, err := store.file.Write(persistentSlice.GetPersistentContents())
	if err != nil {
		return err
	}
	if bytesWritten <= 0 {
		return errors.New("could not append persistentSlice to WAL")
	}
	if bytesWritten < persistentSlice.Size() {
		return errors.New(fmt.Sprintf("%v bytes written to WAL, where as total bytes that should have been written are %v", bytesWritten, persistentSlice.Size()))
	}
	store.size = store.size + uint64(bytesWritten)
	return nil
}

func (store *Store) ReadAll() ([]db.PersistentKeyValuePair, error) {
	var keyValuePairs []db.PersistentKeyValuePair
	var currentOffset int64 = 0

	for currentOffset < int64(store.size) {
		key, value, nextOffset, err := store.readAt(currentOffset)
		if err != nil {
			return nil, err
		}
		keyValuePairs = append(keyValuePairs, db.PersistentKeyValuePair{Key: key, Value: value})
		currentOffset = nextOffset
	}
	return keyValuePairs, nil
}

func (store *Store) readAt(offset int64) (db.PersistentSlice, db.PersistentSlice, int64, error) {
	bytes := make([]byte, int(db.ReservedTotalSize))
	_, err := store.file.ReadAt(bytes, offset)
	if err != nil {
		return db.EmptyPersistentSlice(), db.EmptyPersistentSlice(), -1, err
	}
	sizeToRead := db.ActualTotalSize(bytes)
	contents := make([]byte, sizeToRead)

	_, err = store.file.ReadAt(contents, offset)
	if err != nil {
		return db.EmptyPersistentSlice(), db.EmptyPersistentSlice(), -1, err
	}
	key, value := db.NewPersistentSliceKeyValuePair(contents)
	return key, value, offset + int64(sizeToRead), nil
}

func (store *Store) Size() uint64 {
	return store.size
}

func (store *Store) Close() {
	err := store.file.Close()
	if err != nil {
		log.Default().Println("Error while closing the file " + store.file.Name())
	}
}
