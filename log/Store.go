package log

import (
	"os"
	"path"
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

func (store *Store) Append(persistentSlice PersistentSlice) error {
	_, err := store.file.Write(persistentSlice.GetPersistentContents())
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) ReadAt(offset int64) (PersistentSlice, PersistentSlice, error) {
	bytes := make([]byte, int(ReservedKeySize)+int(ReservedValueSize))
	_, err := store.file.ReadAt(bytes, offset)
	if err != nil {
		return NilPersistentSlice(), NilPersistentSlice(), err
	}
	keySize := ActualKeySize(bytes)
	valueSize := ActualValueSize(bytes[int(ReservedKeySize):])

	contents := make([]byte, keySize+valueSize)
	_, err = store.file.ReadAt(contents, offset+int64(ReservedKeySize)+int64(ReservedValueSize))
	if err != nil {
		return NilPersistentSlice(), NilPersistentSlice(), err
	}
	return NewPersistentSliceUsingRaw(contents[0:keySize]), NewPersistentSliceUsingRaw(contents[keySize:]), nil
}
