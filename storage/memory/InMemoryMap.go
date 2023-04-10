package memory

import (
	"sort"
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage/comparator"
)

type InMemoryMap struct {
	keyValues map[string]model.Slice
}

func NewInMemoryMap() *InMemoryMap {
	return &InMemoryMap{
		keyValues: make(map[string]model.Slice),
	}
}

func (inMemoryMap *InMemoryMap) Put(key model.Slice, value model.Slice) bool {
	keyAsString := key.AsString()
	if _, ok := inMemoryMap.keyValues[keyAsString]; ok {
		return false
	}
	inMemoryMap.keyValues[keyAsString] = value
	return true
}

func (inMemoryMap *InMemoryMap) Get(key model.Slice) model.GetResult {
	keyAsString := key.AsString()
	if value, ok := inMemoryMap.keyValues[keyAsString]; ok {
		return model.GetResult{
			Key:    key,
			Value:  value,
			Exists: true,
		}
	}
	return model.GetResult{
		Key:    key,
		Value:  model.NilSlice(),
		Exists: false,
	}
}

func (inMemoryMap *InMemoryMap) MultiGet(keys []model.Slice) (model.MultiGetResult, []model.Slice) {
	response := model.MultiGetResult{}
	var missingKeys []model.Slice

	for _, key := range keys {
		getResult := inMemoryMap.Get(key)
		if getResult.Exists {
			response.Add(getResult)
		} else {
			missingKeys = append(missingKeys, key)
		}
	}
	return response, missingKeys
}

func (inMemoryMap *InMemoryMap) AllKeyValues(keyComparator comparator.KeyComparator) []model.KeyValuePair {
	var pairs []model.KeyValuePair
	for key, value := range inMemoryMap.keyValues {
		pairs = append(pairs, model.KeyValuePair{Key: model.NewSlice([]byte(key)), Value: value})
	}

	sort.SliceStable(pairs, func(i, j int) bool {
		return keyComparator.Compare(pairs[i].Key, pairs[j].Key) < 0
	})
	return pairs
}
