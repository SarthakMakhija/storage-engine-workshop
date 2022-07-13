package storage

import (
	"errors"
	"log"
	"os"
	"path"
	"storage-engine-workshop/db"
)

type SSTable struct {
	file            *os.File
	filePath        string
	persistentSlice db.PersistentSlice
}

func NewSSTableFrom(memTable *MemTable, directory string) (*SSTable, error) {
	filePath := path.Join(directory, "1.sst")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &SSTable{
		file:            file,
		filePath:        filePath,
		persistentSlice: memTable.AggregatePersistentSlice(),
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
	err = ssTable.file.Close()
	if err != nil {
		log.Default().Println("error while closing the ssTable file " + ssTable.file.Name())
	}
	return nil
}
