package db

import (
	"log"
	"storage-engine-workshop/db/model"
	"testing"
)

func TestBatchPutAllInMemtable(t *testing.T) {
	executor := initRequestExecutor()
	batch := newBatch(executor)

	batch.add(model.NewSlice([]byte("key-1")), model.NewSlice([]byte("value-1")))
	batch.add(model.NewSlice([]byte("key-2")), model.NewSlice([]byte("value-2")))
	batch.add(model.NewSlice([]byte("key-3")), model.NewSlice([]byte("value-3")))

	if err := batch.putInMemtable(); err != nil {
		log.Fatal(err)
	}

	if getResult := <-executor.get(model.NewSlice([]byte("key-1"))); getResult.Value.AsString() != "value-1" {
		t.Fatalf("Expected %v, received %v", "value-1", getResult.Value.AsString())
	}
	if getResult := <-executor.get(model.NewSlice([]byte("key-2"))); getResult.Value.AsString() != "value-2" {
		t.Fatalf("Expected %v, received %v", "value-2", getResult.Value.AsString())
	}
	if getResult := <-executor.get(model.NewSlice([]byte("key-3"))); getResult.Value.AsString() != "value-3" {
		t.Fatalf("Expected %v, received %v", "value-3", getResult.Value.AsString())
	}
}
