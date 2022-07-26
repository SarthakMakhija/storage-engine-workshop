package db

import (
	"storage-engine-workshop/db/model"
)

type KeyValueDb struct {
	configuration Configuration
	executor      *RequestExecutor
}

func NewKeyValueDb(configuration Configuration) (*KeyValueDb, error) {
	workSpace, err := newWorkSpace(configuration)
	if err != nil {
		return nil, err
	}
	return &KeyValueDb{
		configuration: configuration,
		executor:      newRequestExecutor(workSpace),
	}, nil
}

func (db *KeyValueDb) Put(key, value model.Slice) error {
	return <-db.executor.put(key, value)
}

func (db *KeyValueDb) Get(key model.Slice) model.GetResult {
	return <-db.executor.get(key)
}

func (db *KeyValueDb) MultiGet(keys []model.Slice) []model.GetResult {
	return <-db.executor.multiGet(keys)
}
