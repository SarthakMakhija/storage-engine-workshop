package log

import (
	"errors"
	"fmt"
	"os"
	"path"
	"storage-engine-workshop/db"
)

type Store struct {
	file *os.File
}

func NewStore(directory string) (*Store, error) {
	storeFile, err := os.OpenFile(path.Join(directory, "1.store"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{file: storeFile}, nil
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
	return nil
}

func (store *Store) ReadAt(offset int64) (db.PersistentSlice, db.PersistentSlice, error) {
	bytes := make([]byte, int(db.ReservedTotalSize))
	_, err := store.file.ReadAt(bytes, offset)
	if err != nil {
		return db.EmptyPersistentSlice(), db.EmptyPersistentSlice(), err
	}
	sizeToRead := db.ActualTotalSize(bytes)
	contents := make([]byte, sizeToRead)

	_, err = store.file.ReadAt(contents, offset)
	if err != nil {
		return db.EmptyPersistentSlice(), db.EmptyPersistentSlice(), err
	}
	key, value := db.NewPersistentSliceKeyValuePair(contents)
	return key, value, nil
}
