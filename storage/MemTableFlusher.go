package storage

const (
	FAILURE int = iota
	SUCCESS
)

type MemTableFlushStatus struct {
	status int
	err    error
}

type MemTableFlusher struct {
	memTable  *MemTable
	directory string
}

func NewMemTableFlusher(memTable *MemTable, directory string) MemTableFlusher {
	return MemTableFlusher{
		memTable:  memTable,
		directory: directory,
	}
}

func (memTableFlusher MemTableFlusher) Flush() <-chan MemTableFlushStatus {
	response := make(chan MemTableFlushStatus)
	writeErrorToChannel := func(err error) {
		response <- MemTableFlushStatus{status: FAILURE, err: err}
		close(response)
	}
	writeSuccessToChannel := func() {
		response <- MemTableFlushStatus{status: SUCCESS}
		close(response)
	}

	go func() {
		ssTable, err := NewSSTableFrom(memTableFlusher.memTable, memTableFlusher.directory)
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
