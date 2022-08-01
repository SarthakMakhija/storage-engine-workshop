package db

import "storage-engine-workshop/db/model"

type Batch struct {
	executor      *RequestExecutor
	keyValuePairs []model.KeyValuePair
}

func (batch *Batch) add(key, value model.Slice) {
	batch.keyValuePairs = append(batch.keyValuePairs, model.KeyValuePair{Key: key, Value: value})
}

func newBatch(executor *RequestExecutor) *Batch {
	return &Batch{executor: executor, keyValuePairs: []model.KeyValuePair{}}
}

func (batch *Batch) putInMemtable() error {
	for _, keyValuePair := range batch.keyValuePairs {
		err := <-batch.executor.put(keyValuePair.Key, keyValuePair.Value)
		if err != nil {
			//Delete already added key/value
			return err
		}
	}
	return nil
}

func (batch *Batch) isEmpty() bool {
	return len(batch.keyValuePairs) == 0
}
