package storage

import (
	"errors"
	"os"
	"path"
	"storage-engine-workshop/db"
)

type SSTable struct {
	file            *os.File
	persistentSlice db.PersistentSlice
}

func NewSSTableFrom(memTable *MemTable, directory string) (*SSTable, error) {
	file, err := os.OpenFile(path.Join(directory, "1.sst"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &SSTable{
		file:            file,
		persistentSlice: memTable.AggregatedPersistentSlice(),
	}, nil
}

func (ssTable *SSTable) Write() error {
	bytesWritten, err := ssTable.file.WriteAt(ssTable.persistentSlice.GetPersistentContents(), 0)
	if err != nil {
		return err
	}
	if bytesWritten <= 0 {
		return errors.New("could not dump persistent slice to SSTable")
	}
	return nil
}
