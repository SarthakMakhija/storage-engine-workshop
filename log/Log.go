package log

import "storage-engine-workshop/db"

type WAL struct {
	directory string
	store     *Store
}

func NewLog(directory string, pageSize int) (*WAL, error) {
	store, err := NewStore(directory, pageSize)
	if err != nil {
		return nil, err
	}
	return &WAL{directory: directory, store: store}, nil
}

func (log *WAL) Append(putCommand PutCommand) error {
	if err := log.store.Append(db.NewPersistentSlice(db.KeyValuePair{Key: putCommand.key, Value: putCommand.value})); err != nil {
		return err
	}
	return nil
}

func (log WAL) ReadAt(offset int64) (PutCommand, error) {
	key, value, err := log.store.ReadAt(offset)
	if err != nil {
		return PutCommand{}, err
	}
	return NewPutCommand(key.GetSlice(), value.GetSlice()), nil
}
