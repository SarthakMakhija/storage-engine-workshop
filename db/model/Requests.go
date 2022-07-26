package model

type PutRequest struct {
	Key, Value      Slice
	ResponseChannel chan error
}

type GetRequest struct {
	Key             Slice
	ResponseChannel chan GetResult
}

type MultiGetRequest struct {
	Keys            []Slice
	ResponseChannel chan []GetResult
}
