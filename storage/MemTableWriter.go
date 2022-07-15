package storage

import (
	"storage-engine-workshop/storage/memory"
	"storage-engine-workshop/storage/sst"
)

const (
	SUCCESS int = iota
	FAILURE
)

type MemTableWriteStatus struct {
	status int
	err    error
}

type MemTableWriter struct {
	memTable  *memory.MemTable
	ssTables  *sst.SSTables
	ssTable   *sst.SSTable
	directory string
}

func NewMemTableWriter(memTable *memory.MemTable, directory string) (*MemTableWriter, error) {
	ssTables, err := sst.NewSSTables(directory)
	if err != nil {
		return nil, err
	}
	return &MemTableWriter{
		memTable:  memTable,
		directory: directory,
		ssTables:  ssTables,
	}, nil
}

func (memTableWriter *MemTableWriter) Write() <-chan MemTableWriteStatus {
	response := make(chan MemTableWriteStatus)

	go func() {
		err := memTableWriter.mutateWithSsTable()
		if err != nil {
			writeErrorToChannel(err, response)
			return
		}
		if err := memTableWriter.ssTable.Write(); err != nil {
			writeErrorToChannel(err, response)
			return
		}
		writeSuccessToChannel(response)
	}()
	return response
}

func (memTableWriter *MemTableWriter) mutateWithSsTable() error {
	ssTable, err := memTableWriter.ssTables.NewSSTable(memTableWriter.memTable)
	if err != nil {
		return err
	}
	memTableWriter.ssTable = ssTable
	return nil
}

func writeErrorToChannel(err error, statusChannel chan MemTableWriteStatus) {
	statusChannel <- MemTableWriteStatus{status: FAILURE, err: err}
	close(statusChannel)
}

func writeSuccessToChannel(statusChannel chan MemTableWriteStatus) {
	statusChannel <- MemTableWriteStatus{status: SUCCESS}
	close(statusChannel)
}
