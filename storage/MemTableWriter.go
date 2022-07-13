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
	ssTable   *sst.SSTable
	directory string
}

func NewMemTableWriter(memTable *memory.MemTable, directory string) (*MemTableWriter, error) {
	ssTable, err := sst.NewSSTableFrom(memTable, directory)
	if err != nil {
		return nil, err
	}
	return &MemTableWriter{
		ssTable:   ssTable,
		directory: directory,
	}, nil
}

func (memTableWriter MemTableWriter) Write() <-chan MemTableWriteStatus {
	response := make(chan MemTableWriteStatus)

	go func() {
		writeErrorToChannel := func(err error) {
			response <- MemTableWriteStatus{status: FAILURE, err: err}
			close(response)
		}
		writeSuccessToChannel := func() {
			response <- MemTableWriteStatus{status: SUCCESS}
			close(response)
		}
		if err := memTableWriter.ssTable.Write(); err != nil {
			writeErrorToChannel(err)
			return
		}
		writeSuccessToChannel()
	}()
	return response
}
