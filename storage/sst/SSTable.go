package sst

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/memory"
)

type SSTable struct {
	file            *os.File
	filePath        string
	totalKeys       int
	persistentSlice db.PersistentSlice
}

func NewSSTableFrom(memTable *memory.MemTable, directory string) (*SSTable, error) {
	filePath := path.Join(directory, "1.sst")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	persistentSlice, totalKeys := memTable.AggregatePersistentSlice()
	return &SSTable{
		file:            file,
		filePath:        filePath,
		totalKeys:       totalKeys,
		persistentSlice: persistentSlice,
	}, nil
}

func (ssTable *SSTable) Write() error {
	bytesWritten, err := ssTable.file.WriteAt(ssTable.persistentSlice.GetPersistentContents(), 0)
	if err != nil {
		return err
	}
	if bytesWritten <= 0 {
		return errors.New(fmt.Sprintf("%v bytes written to SSTable, could not dump persistent slice to SSTable", bytesWritten))
	}
	if bytesWritten < ssTable.persistentSlice.Size() {
		return errors.New(fmt.Sprintf("%v bytes written to SSTable, where as total bytes that should have been written are %v", bytesWritten, ssTable.persistentSlice.Size()))
	}
	if err := ssTable.file.Sync(); err != nil {
		return errors.New("error while syncing the ssTable file " + ssTable.file.Name())
	}
	if err := ssTable.file.Close(); err != nil {
		log.Default().Println("error while closing the ssTable file " + ssTable.file.Name())
	}
	return nil
}
