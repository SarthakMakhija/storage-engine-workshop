package db

import (
	"errors"
	"fmt"
	"storage-engine-workshop/db/model"
)

type Transaction struct {
	executor *RequestExecutor
	batch    *Batch
}

type ReadonlyTransaction struct {
	executor *RequestExecutor
}

const (
	maxSizeAllowedBytes uint16 = 65535
)

func newTransaction(executor *RequestExecutor) *Transaction {
	return &Transaction{
		executor: executor,
		batch:    NewBatch(),
	}
}

func newReadonlyTransaction(executor *RequestExecutor) ReadonlyTransaction {
	return ReadonlyTransaction{
		executor: executor,
	}
}

func (txn *Transaction) Put(key, value model.Slice) error {
	if txn.batch.isTotalSizeGreaterThan(maxSizeAllowedBytes) {
		return errors.New(fmt.Sprintf("can not add more than the total key/value pair size %v in a transaction", maxSizeAllowedBytes))
	}
	txn.batch.add(key, value)
	return nil
}

func (txn *Transaction) Commit() error {
	if txn.batch.isEmpty() {
		return errors.New("nothing to commit, put key/value before committing")
	}
	return <-txn.executor.put(txn.batch)
}

func (txn ReadonlyTransaction) Get(key model.Slice) model.GetResult {
	return <-txn.executor.get(key)
}

func (txn ReadonlyTransaction) MultiGet(keys []model.Slice) []model.GetResult {
	return <-txn.executor.multiGet(keys)
}
