package db

import (
	"storage-engine-workshop/db/model"
	"storage-engine-workshop/log"
	"storage-engine-workshop/storage"
	"storage-engine-workshop/storage/memory"
	"storage-engine-workshop/storage/sst"
)

type Workspace struct {
	log              *log.WAL
	ssTables         *sst.SSTables
	activeMemTable   *memory.MemTable
	inactiveMemTable *memory.MemTable
	configuration    Configuration
}

func newWorkSpace(configuration Configuration) (*Workspace, error) {
	wal, err := log.NewLog(configuration.directory, configuration.segmentMaxSizeBytes)
	if err != nil {
		return nil, err
	}
	ssTables, err := sst.NewSSTables(configuration.directory)
	if err != nil {
		return nil, err
	}
	return &Workspace{
		log:            wal,
		ssTables:       ssTables,
		activeMemTable: memory.NewMemTable(32, configuration.keyComparator),
		configuration:  configuration,
	}, nil
}

func (workspace *Workspace) put(key model.Slice, value model.Slice) error {
	err := workspace.log.Append(log.NewPutCommand(model.KeyValuePair{Key: key, Value: value}))
	if err != nil {
		return err
	}
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
	memTables := []*memory.MemTable{workspace.activeMemTable, workspace.inactiveMemTable}

	index, allGetResults := 0, make([]model.GetResult, len(keys))
	var pendingKeys []model.Slice

	for _, memTable := range memTables {
		if memTable != nil {
			for _, getResult := range memTable.MultiGet(keys).Values {
				if getResult.Exists {
					allGetResults[index] = getResult
					index = index + 1
				} else {
					pendingKeys = append(pendingKeys, getResult.Key)
				}
			}
		}
	}
	if len(pendingKeys) > 0 {
		for _, getResult := range workspace.ssTables.MultiGet(pendingKeys, workspace.configuration.keyComparator).Values {
			allGetResults[index] = getResult
			index = index + 1
		}
	}
	return allGetResults
}
