package sst

import (
	"encoding/binary"
	"storage-engine-workshop/db"
	"storage-engine-workshop/storage/comparator"
	"unsafe"
)

var (
	bigEndian          = binary.BigEndian
	ReservedOffsetSize = unsafe.Sizeof(uint64(0))
	ReservedKeySize    = unsafe.Sizeof(uint32(0))
)

type IndexBlock struct {
	store *Store
}

func NewIndexBlock(store *Store) *IndexBlock {
	return &IndexBlock{
		store: store,
	}
}

func (indexBlock *IndexBlock) Write(beginOffsetByKey []int64, blockBeginOffset int64, keyValuePairs []db.KeyValuePair) error {
	offset, indexBlockBeginOffset := blockBeginOffset, blockBeginOffset

	for index, keyValuePair := range keyValuePairs {
		bytes := indexBlock.marshal(keyValuePair.Key, beginOffsetByKey[index])
		if bytesWritten, err := indexBlock.store.WriteAt(bytes, offset); err != nil {
			return err
		} else {
			offset = offset + int64(bytesWritten)
		}
	}
	bytes := make([]byte, ReservedOffsetSize)
	bigEndian.PutUint64(bytes, uint64(indexBlockBeginOffset))

	_, err := indexBlock.store.WriteAt(bytes, offset)
	return err
}

func (indexBlock *IndexBlock) GetKeyOffset(key db.Slice, keyComparator comparator.KeyComparator) (int64, error) {
	blockBytes, err := indexBlock.readIndexBlock()
	if err != nil {
		return -1, err
	}
	index := 0
	for index < len(blockBytes) {
		actualKeySize := bigEndian.Uint32(blockBytes[index:])
		keyBeginIndex := index + int(ReservedKeySize) + int(ReservedOffsetSize)
		serializedKey := blockBytes[keyBeginIndex : keyBeginIndex+int(actualKeySize)]
		if keyComparator.Compare(db.NewSlice(serializedKey), key) == 0 {
			keyOffset := bigEndian.Uint64(blockBytes[(index + int(ReservedKeySize)):])
			return int64(keyOffset), nil
		}
		index = index + int(ReservedKeySize) + int(ReservedOffsetSize) + int(actualKeySize)
	}
	return -1, nil
}

func (indexBlock *IndexBlock) readIndexBlock() ([]byte, error) {
	size, _ := indexBlock.store.Size()
	offsetContainingIndexBlockSize := size - int64(ReservedOffsetSize)
	_, err := indexBlock.store.SeekFromBeginning(offsetContainingIndexBlockSize)
	if err != nil {
		return nil, err
	}
	indexBlockBeginOffsetBytes := make([]byte, int(ReservedOffsetSize))
	_, err = indexBlock.store.ReadAt(indexBlockBeginOffsetBytes, offsetContainingIndexBlockSize)
	if err != nil {
		return nil, err
	}
	indexBlockBeginOffset := bigEndian.Uint64(indexBlockBeginOffsetBytes)
	blockBytes := make([]byte, offsetContainingIndexBlockSize-int64(indexBlockBeginOffset))
	_, err = indexBlock.store.ReadAt(blockBytes, int64(indexBlockBeginOffset))
	if err != nil {
		return nil, err
	}
	return blockBytes, nil
}

func (indexBlock *IndexBlock) marshal(key db.Slice, keyBeginOffset int64) []byte {
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