package db

import (
	"fmt"
	"os"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
	"sync"
	"testing"
	"time"
)

func initRequestExecutor() *RequestExecutor {
	const segmentMaxSizeBytes uint64 = 32
	const bufferMaxSizeBytes uint64 = 1024

	directory := tempDirectory()
	defer os.RemoveAll(directory)

	configuration := NewConfiguration(directory, segmentMaxSizeBytes, bufferMaxSizeBytes, comparator.StringKeyComparator{})
	workSpace, _ := newWorkSpace(configuration)

	return newRequestExecutor(workSpace)
}

func TestPutFollowedByGet(t *testing.T) {
	var wg sync.WaitGroup
	executor := initRequestExecutor()

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-executor.put(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
	}()

	time.Sleep(100 * time.Millisecond)

	go func() {
		defer wg.Done()
		getResult := <-executor.get(model.NewSlice([]byte("Company")))
		if getResult.Value.AsString() != "TW" {
			t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", "TW", getResult.Value.AsString()))
		}
	}()

	wg.Wait()
}

func TestPutAndGetConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	executor := initRequestExecutor()

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-executor.put(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
	}()

	go func() {
		defer wg.Done()
		getResult := <-executor.get(model.NewSlice([]byte("Company")))
		if getResult.Exists && getResult.Value.AsString() != "TW" {
			t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", "TW", getResult.Value.AsString()))
		}
	}()

	wg.Wait()
}

func TestMultiGetFollowedByPut(t *testing.T) {
	var wg sync.WaitGroup
	executor := initRequestExecutor()

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-executor.put(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
		<-executor.put(model.NewSlice([]byte("Field")), model.NewSlice([]byte("Storage engine")))
	}()

	time.Sleep(100 * time.Millisecond)

	go func() {
		defer wg.Done()
		expectedValueByKey := map[string]string{
			"Company": "TW",
			"Field":   "Storage engine",
		}
		multiGetResult := <-executor.multiGet([]model.Slice{model.NewSlice([]byte("Company")), model.NewSlice([]byte("Field"))})
		for _, result := range multiGetResult {
			if result.Value.AsString() != expectedValueByKey[result.Key.AsString()] {
				t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", expectedValueByKey[result.Key.AsString()], result.Value.AsString()))
			}
		}
	}()

	wg.Wait()
}

func TestPutAndMultiGetConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	executor := initRequestExecutor()

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-executor.put(model.NewSlice([]byte("Company")), model.NewSlice([]byte("TW")))
		<-executor.put(model.NewSlice([]byte("Field")), model.NewSlice([]byte("Storage engine")))
	}()

	go func() {
		defer wg.Done()
		expectedValueByKey := map[string]string{
			"Company": "TW",
			"Field":   "Storage engine",
		}
		multiGetResult := <-executor.multiGet([]model.Slice{model.NewSlice([]byte("Company")), model.NewSlice([]byte("Field"))})
		for _, result := range multiGetResult {
			if result.Exists && result.Value.AsString() != expectedValueByKey[result.Key.AsString()] {
				t.Errorf(fmt.Sprintf("Expected value to be %v, received %v", expectedValueByKey[result.Key.AsString()], result.Value.AsString()))
			}
		}
	}()

	wg.Wait()
}
