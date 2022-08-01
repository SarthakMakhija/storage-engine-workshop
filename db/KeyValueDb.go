package db

import (
	"storage-engine-workshop/log"
)

type KeyValueDb struct {
	log      *log.WAL
	executor *RequestExecutor
}

func NewKeyValueDb(configuration Configuration) (*KeyValueDb, error) {
	wal, err := log.NewLog(configuration.directory, configuration.segmentMaxSizeBytes)
	if err != nil {
		return nil, err
	}
	workSpace, err := newWorkSpace(configuration)
	if err != nil {
		return nil, err
	}
	return &KeyValueDb{
		log:      wal,
		executor: newRequestExecutor(workSpace),
	}, nil
}

func (db *KeyValueDb) newTransaction() *Transaction {
	return newTransaction(db.log, db.executor)
}

func (db *KeyValueDb) newReadonlyTransaction() ReadonlyTransaction {
	return newReadonlyTransaction(db.executor)
}
