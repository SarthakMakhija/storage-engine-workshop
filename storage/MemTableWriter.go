package storage

import (
	"storage-engine-workshop/storage/memory"
	"storage-engine-workshop/storage/sst"
)

const (
	FAILURE int = iota
	SUCCESS
)

type MemTableWriteStatus struct {
	status int
	err    error
}

type MemTableWriter struct {
	memTable  *memory.MemTable
	directory string
}

func NewMemTableWriter(memTable *memory.MemTable, directory string) MemTableWriter {
	return MemTableWriter{
		memTable:  memTable,
		directory: directory,
	}
}

func (memTableWriter MemTableWriter) Write() <-chan MemTableWriteStatus {
	response := make(chan MemTableWriteStatus)
	writeErrorToChannel := func(err error) {
		response <- MemTableWriteStatus{status: FAILURE, err: err}
		close(response)
	}
	writeSuccessToChannel := func() {
		response <- MemTableWriteStatus{status: SUCCESS}
		close(response)
	}

	go func() {
		ssTable, err := sst.NewSSTableFrom(memTableWriter.memTable, memTableWriter.directory)
		if err != nil {
			writeErrorToChannel(err)
			return
		}
		if err := ssTable.Write(); err != nil {
			writeErrorToChannel(err)
			return
		}
		writeSuccessToChannel()
	}()
	return response
}
