package db

import (
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/log"
)

type Transaction struct {
	log   *log.WAL
	batch *Batch
}

type ReadonlyTransaction struct {
	executor *RequestExecutor
}

func newTransaction(log *log.WAL, executor *RequestExecutor) *Transaction {
	return &Transaction{
		log:   log,
		batch: newBatch(executor),
	}
}

func newReadonlyTransaction(executor *RequestExecutor) ReadonlyTransaction {
	return ReadonlyTransaction{
		executor: executor,
	}
}

func (txn *Transaction) Put(key, value model.Slice) error {
	err := txn.log.Append(log.NewPutCommand(model.KeyValuePair{Key: key, Value: value}))
	if err != nil {
		return err
	}
	txn.batch.add(key, value)
	return nil
}

func (txn *Transaction) Commit() error {
	err := txn.batch.putInMemtable()
	if err != nil {
		_ = txn.log.MarkTransactionWith(log.TransactionStatusFailed)
		return err
	}
	return txn.log.MarkTransactionWith(log.TransactionStatusSuccess)
}

func (txn ReadonlyTransaction) Get(key model.Slice) model.GetResult {
	return <-txn.executor.get(key)
}

func (txn ReadonlyTransaction) MultiGet(keys []model.Slice) []model.GetResult {
	return <-txn.executor.multiGet(keys)
}
