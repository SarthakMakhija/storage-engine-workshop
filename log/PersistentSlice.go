package log

import (
	"encoding/binary"
	"storage-engine-workshop/db"
	"unsafe"
)

var (
	bigEndian         = binary.BigEndian
	ReservedTotalSize = unsafe.Sizeof(uint32(0))
	ReservedKeySize   = unsafe.Sizeof(uint32(0))
)

type PersistentSlice struct {
	Contents []byte
}

var emptySlice = PersistentSlice{}

func NilPersistentSlice() PersistentSlice {
	return emptySlice
}

func NewPersistentSlice(putCommand PutCommand) PersistentSlice {
	return marshal(putCommand)
}

func NewPersistentSliceKeyValuePair(contents []byte) (PersistentSlice, PersistentSlice) {
	return unmarshal(contents)
}

func (persistentSlice PersistentSlice) GetPersistentContents() []byte {
	return persistentSlice.Contents
}

func (persistentSlice PersistentSlice) GetSlice() db.Slice {
	return db.NewSlice(persistentSlice.GetPersistentContents())
}

func (persistentSlice PersistentSlice) Size() int {
	return len(persistentSlice.Contents)
}

func ActualTotalSize(bytes []byte) uint32 {
	return bigEndian.Uint32(bytes)
}

func marshal(putCommand PutCommand) PersistentSlice {
	reservedTotalSize, reservedKeySize := ReservedTotalSize, ReservedKeySize
	actualTotalSize :=
		len(putCommand.key.GetRawContent()) +
			len(putCommand.value.GetRawContent()) +
			int(reservedKeySize) +
			int(reservedTotalSize)

	//The way PutCommand is encoded is: 4 bytes for totalSize | 4 bytes for keySize | Key content | Value content
	bytes := make([]byte, actualTotalSize)
	offset := 0

	bigEndian.PutUint32(bytes, uint32(actualTotalSize))
	offset = offset + int(reservedTotalSize)

	bigEndian.PutUint32(bytes[offset:], uint32(len(putCommand.key.GetRawContent())))
	offset = offset + int(reservedKeySize)

	copy(bytes[offset:], putCommand.key.GetRawContent())
	offset = offset + len(putCommand.key.GetRawContent())

	copy(bytes[offset:], putCommand.value.GetRawContent())
	return PersistentSlice{Contents: bytes}
}

func unmarshal(bytes []byte) (PersistentSlice, PersistentSlice) {
	keySize := bigEndian.Uint32(bytes)
	keyEndOffset := uint32(ReservedKeySize) + keySize

	return PersistentSlice{Contents: bytes[ReservedKeySize:keyEndOffset]}, PersistentSlice{Contents: bytes[keyEndOffset:]}
}
