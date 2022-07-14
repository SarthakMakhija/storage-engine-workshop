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
	ssTable   *sst.SSTable
	directory string
}

func NewMemTableWriter(memTable *memory.MemTable, directory string) (*MemTableWriter, error) {
	return &MemTableWriter{
		memTable:  memTable,
		directory: directory,
		ssTable:   nil,
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
	ssTable, err := sst.NewSSTableFrom(memTableWriter.memTable, memTableWriter.directory)
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
