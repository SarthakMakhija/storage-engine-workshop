package db

import (
	"storage-engine-workshop/db/model"
)

type RequestExecutor struct {
	requestChannel chan interface{}
	workSpace      *Workspace
}

func newRequestExecutor(workSpace *Workspace) *RequestExecutor {
	executor := &RequestExecutor{
		requestChannel: make(chan interface{}),
		workSpace:      workSpace,
	}
	executor.init()
	return executor
}

func (executor *RequestExecutor) init() {
	put := func(putRequest PutRequest) {
		putRequest.ResponseChannel <- executor.workSpace.put(putRequest.Batch)
		close(putRequest.ResponseChannel)
	}
	get := func(getRequest GetRequest) {
		getRequest.ResponseChannel <- executor.workSpace.get(getRequest.Key)
		close(getRequest.ResponseChannel)
	}
	multiGet := func(multiGetRequest MultiGetRequest) {
		multiGetRequest.ResponseChannel <- executor.workSpace.multiGet(multiGetRequest.Keys)
		close(multiGetRequest.ResponseChannel)
	}

	go func() {
		for {
			request := <-executor.requestChannel
			if putRequest, ok := request.(PutRequest); ok {
				put(putRequest)
			} else if getRequest, ok := request.(GetRequest); ok {
				get(getRequest)
			} else if multiGetRequest, ok := request.(MultiGetRequest); ok {
				multiGet(multiGetRequest)
			}
		}
	}()
}

func (executor *RequestExecutor) put(batch *Batch) chan error {
	responseChannel := make(chan error)
	executor.requestChannel <- PutRequest{Batch: batch, ResponseChannel: responseChannel}
	return responseChannel
}

func (executor *RequestExecutor) get(key model.Slice) chan model.GetResult {
	responseChannel := make(chan model.GetResult)
	executor.requestChannel <- GetRequest{Key: key, ResponseChannel: responseChannel}
	return responseChannel
}

func (executor *RequestExecutor) multiGet(keys []model.Slice) chan []model.GetResult {
	responseChannel := make(chan []model.GetResult)
	executor.requestChannel <- MultiGetRequest{Keys: keys, ResponseChannel: responseChannel}
	return responseChannel
}
