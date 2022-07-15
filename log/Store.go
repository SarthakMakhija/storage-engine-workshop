package log

import (
	"errors"
	"fmt"
	"log"
	"os"
)

type Store struct {
	file *os.File
	size int64
}

func NewStore(filePath string) (*Store, error) {
	storeFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := storeFile.Stat()
	if err != nil {
		return nil, err
	}
	return &Store{file: storeFile, size: stat.Size()}, nil
}

func (store *Store) Append(persistentLogSlice PersistentLogSlice) error {
	bytesWritten, err := store.file.Write(persistentLogSlice.GetPersistentContents())
	if err != nil {
		return err
	}
	if bytesWritten <= 0 {
		return errors.New("could not append persistentLogSlice to WAL")
	}
	if bytesWritten < persistentLogSlice.Size() {
		return errors.New(fmt.Sprintf("%v bytes written to WAL, where as total bytes that should have been written are %v", bytesWritten, persistentLogSlice.Size()))
	}
	store.size = store.size + int64(bytesWritten)
	return nil
}

func (store *Store) ReadAll() ([]PersistentKeyValuePair, error) {
	var keyValuePairs []PersistentKeyValuePair
	var currentOffset int64 = 0

	for currentOffset < store.size {
		key, value, nextOffset, err := store.readAt(currentOffset)
		if err != nil {
			return nil, err
		}
		keyValuePairs = append(keyValuePairs, PersistentKeyValuePair{Key: key, Value: value})
		currentOffset = nextOffset
	}
	return keyValuePairs, nil
}

func (store *Store) Size() int64 {
	return store.size
}

func (store *Store) Close() {
	err := store.file.Close()
	if err != nil {
		log.Default().Println("Error while closing the file " + store.file.Name())
	}
}

func (store *Store) readAt(offset int64) (PersistentLogSlice, PersistentLogSlice, int64, error) {
	bytes := make([]byte, int(ReservedTotalSize))
	_, err := store.file.ReadAt(bytes, offset)
	if err != nil {
		return EmptyPersistentLogSlice(), EmptyPersistentLogSlice(), -1, err
	}
	sizeToRead := ActualTotalSize(bytes)
	contents := make([]byte, sizeToRead)

	_, err = store.file.ReadAt(contents, offset)
	if err != nil {
		return EmptyPersistentLogSlice(), EmptyPersistentLogSlice(), -1, err
	}
	key, value := NewPersistentLogSliceKeyValuePair(contents)
	return key, value, offset + int64(sizeToRead), nil
}
