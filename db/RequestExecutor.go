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
	put := func(putRequest model.PutRequest) {
		putRequest.ResponseChannel <- executor.workSpace.put(putRequest.Key, putRequest.Value)
		close(putRequest.ResponseChannel)
	}
	get := func(getRequest model.GetRequest) {
		getRequest.ResponseChannel <- executor.workSpace.get(getRequest.Key)
		close(getRequest.ResponseChannel)
	}
	multiGet := func(multiGetRequest model.MultiGetRequest) {
		multiGetRequest.ResponseChannel <- executor.workSpace.multiGet(multiGetRequest.Keys)
		close(multiGetRequest.ResponseChannel)
	}

	go func() {
		for {
			request := <-executor.requestChannel
			if putRequest, ok := request.(model.PutRequest); ok {
				put(putRequest)
			} else if getRequest, ok := request.(model.GetRequest); ok {
				get(getRequest)
			} else if multiGetRequest, ok := request.(model.MultiGetRequest); ok {
				multiGet(multiGetRequest)
			}
		}
	}()
}

func (executor *RequestExecutor) put(key, value model.Slice) chan error {
	responseChannel := make(chan error)
	executor.requestChannel <- model.PutRequest{Key: key, Value: value, ResponseChannel: responseChannel}
	return responseChannel
}

func (executor *RequestExecutor) get(key model.Slice) chan model.GetResult {
	responseChannel := make(chan model.GetResult)
	executor.requestChannel <- model.GetRequest{Key: key, ResponseChannel: responseChannel}
	return responseChannel
}

func (executor *RequestExecutor) multiGet(keys []model.Slice) chan []model.GetResult {
	responseChannel := make(chan []model.GetResult)
	executor.requestChannel <- model.MultiGetRequest{Keys: keys, ResponseChannel: responseChannel}
	return responseChannel
}
