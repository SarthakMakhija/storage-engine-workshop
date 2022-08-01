package db

import (
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/storage"
	"storage-engine-workshop/storage/memory"
	"storage-engine-workshop/storage/sst"
)

type Workspace struct {
	ssTables         *sst.SSTables
	activeMemTable   *memory.MemTable
	inactiveMemTable *memory.MemTable
	configuration    Configuration
}

func newWorkSpace(configuration Configuration) (*Workspace, error) {
	ssTables, err := sst.NewSSTables(configuration.directory)
	if err != nil {
		return nil, err
	}
	return &Workspace{
		ssTables:       ssTables,
		activeMemTable: memory.NewMemTable(32, configuration.keyComparator),
		configuration:  configuration,
	}, nil
}

func (workspace *Workspace) put(key model.Slice, value model.Slice) error {
	writeToSSTable := func() {
		//handle error
		storage.NewMemTableWriter(workspace.activeMemTable, workspace.ssTables).Write()
	}
	mayBeSwapMemTable := func() {
		if workspace.activeMemTable.TotalSize() >= workspace.configuration.bufferSizeBytes {
			writeToSSTable()
			workspace.inactiveMemTable = workspace.activeMemTable
			workspace.activeMemTable = memory.NewMemTable(32, workspace.configuration.keyComparator)
		}
	}
	mayBeSwapMemTable()
	workspace.activeMemTable.Put(key, value)
	return nil
}

func (workspace *Workspace) get(key model.Slice) model.GetResult {
	memTables := []*memory.MemTable{workspace.activeMemTable, workspace.inactiveMemTable}
	get := func(memTable *memory.MemTable) model.GetResult {
		return memTable.Get(key)
	}
	for _, memTable := range memTables {
		if memTable != nil {
			if getResult := get(memTable); getResult.Exists {
				return getResult
			}
		}
	}
	return workspace.ssTables.Get(key, workspace.configuration.keyComparator)
}

func (workspace *Workspace) multiGet(keys []model.Slice) []model.GetResult {
	index, allGetResults := 0, make([]model.GetResult, len(keys))

	buildResult := func(multiGetResult model.MultiGetResult) {
		for _, getResult := range multiGetResult.Values {
			if getResult.Exists {
				allGetResults[index] = getResult
				index = index + 1
			}
		}
	}
	multiGetIn := func(memTable *memory.MemTable, keys []model.Slice) []model.Slice {
		if memTable != nil {
			multiGetResult, missingKeys := workspace.activeMemTable.MultiGet(keys)
			buildResult(multiGetResult)
			return missingKeys
		}
		return []model.Slice{}
	}

	missingKeys := multiGetIn(workspace.activeMemTable, keys)
	missingKeys = multiGetIn(workspace.inactiveMemTable, missingKeys)

	if len(missingKeys) > 0 {
		getResults := workspace.ssTables.MultiGet(missingKeys, workspace.configuration.keyComparator).Values
		for _, getResult := range getResults {
			allGetResults[index] = getResult
			index = index + 1
		}
	}
	return allGetResults
}
