package sst

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/filter"
	"storage-engine-workshop/storage/memory"
	"unsafe"
)

var (
	bigEndian          = binary.BigEndian
	ReservedOffsetSize = unsafe.Sizeof(uint64(0))
	ReservedKeySize    = unsafe.Sizeof(uint32(0))
)

type SSTable struct {
	file          *os.File
	totalKeys     int
	keyValuePairs []db.KeyValuePair
	bloomFilter   *filter.BloomFilter
}

func NewSSTableFrom(memTable *memory.MemTable, directory string) (*SSTable, error) {
	ssTableFile, err := createSSTableFile(path.Join(directory, "1.sst"))
	if err != nil {
		return nil, err
	}
	bloomFilter, err := createBloomFilter(directory, "1", memTable.TotalKeys())
	if err != nil {
		return nil, err
	}
	return &SSTable{
		file:          ssTableFile,
		totalKeys:     memTable.TotalKeys(),
		bloomFilter:   bloomFilter,
		keyValuePairs: memTable.AllKeyValues(),
	}, nil
}

func (ssTable *SSTable) Write() error {
	if len(ssTable.keyValuePairs) == 0 {
		return errors.New("ssTable does not contain any key value pairs to write to " + ssTable.file.Name())
	}
	beginOffsetByKey, offset, err := ssTable.writeKeyValues()
	if err != nil {
		return err
	}
	if _, err := ssTable.writeIndexBlock(beginOffsetByKey, offset); err != nil {
		return err
	}
	if err := ssTable.file.Sync(); err != nil {
		return errors.New("error while syncing the ssTable file " + ssTable.file.Name())
	}
	if err := ssTable.file.Close(); err != nil {
		log.Default().Println("error while closing the ssTable file " + ssTable.file.Name())
	}
	return nil
}

func (ssTable *SSTable) writeKeyValues() ([]int64, int64, error) {
	var offset int64 = 0
	beginOffsetByKey := make([]int64, len(ssTable.keyValuePairs))

	for index, keyValuePair := range ssTable.keyValuePairs {
		if bytesWritten, err := ssTable.writeAt(db.NewPersistentSlice(keyValuePair).GetPersistentContents(), offset); err != nil {
			return nil, 0, err
		} else {
			offset = offset + int64(bytesWritten)
			beginOffsetByKey[index] = offset
		}
		if err := ssTable.bloomFilter.Put(keyValuePair.Key); err != nil {
			return nil, 0, err
		}
	}
	return beginOffsetByKey, offset, nil
}

func (ssTable *SSTable) writeIndexBlock(beginOffsetByKey []int64, blockBeginOffset int64) (int64, error) {
	offset, indexBlockBeginOffset := blockBeginOffset, blockBeginOffset

	for index, keyValuePair := range ssTable.keyValuePairs {
		bytes := marshal(keyValuePair.Key, beginOffsetByKey[index])
		if bytesWritten, err := ssTable.writeAt(bytes, offset); err != nil {
			return 0, err
		} else {
			offset = offset + int64(bytesWritten)
		}
	}
	bytes := make([]byte, ReservedOffsetSize)
	bigEndian.PutUint64(bytes, uint64(indexBlockBeginOffset))

	nextOffset, err := ssTable.writeAt(bytes, offset)
	return int64(nextOffset), err
}

func (ssTable *SSTable) writeAt(bytes []byte, offset int64) (int, error) {
	bytesWritten, err := ssTable.file.WriteAt(bytes, offset)
	if err != nil {
		return 0, err
	}
	if bytesWritten <= 0 {
		return 0, errors.New(fmt.Sprintf("%v bytes written to SSTable, could not dump persistent persistentSlice to SSTable", bytesWritten))
	}
	if bytesWritten < len(bytes) {
		return 0, errors.New(fmt.Sprintf("%v bytes written to SSTable, where as total bytes that should have been written are %v", bytesWritten, len(bytes)))
	}
	return bytesWritten, nil
}

func marshal(key db.Slice, keyBeginOffset int64) []byte {
	actualTotalSize := uint64(ReservedKeySize) + uint64(ReservedOffsetSize) + uint64(key.Size())

	//The way index block is encoded is: 4 bytes for keySize | 8 bytes for offsetSize | Key content
	bytes := make([]byte, actualTotalSize)
	index := 0

	bigEndian.PutUint32(bytes, uint32(key.Size()))
	index = index + int(ReservedKeySize)

	bigEndian.PutUint64(bytes[index:], uint64(keyBeginOffset))
	index = index + int(ReservedOffsetSize)

	copy(bytes[index:], key.GetRawContent())
	return bytes
}

func createSSTableFile(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func createBloomFilter(directory string, fileNamePrefix string, totalKeys int) (*filter.BloomFilter, error) {
	bloomFilters, err := filter.NewBloomFilters(directory, 0.001)
	if err != nil {
		return nil, err
	}
	bloomFilter, err := bloomFilters.NewBloomFilter(filter.BloomFilterOptions{
		Capacity:       totalKeys,
		FileNamePrefix: fileNamePrefix,
	})
	if err != nil {
		return nil, err
	}
	return bloomFilter, nil
}
