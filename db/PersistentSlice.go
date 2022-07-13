package db

import (
	"encoding/binary"
	"unsafe"
)

var (
	bigEndian         = binary.BigEndian
	ReservedTotalSize = unsafe.Sizeof(uint32(0))
	ReservedKeySize   = unsafe.Sizeof(uint32(0))
)

type PersistentSlice struct {
	contents []byte
}

var emptyPersistentSlice = PersistentSlice{contents: []byte{}}

func EmptyPersistentSlice() PersistentSlice {
	return emptyPersistentSlice
}

func NewPersistentSlice(keyValuePair KeyValuePair) PersistentSlice {
	return marshal(keyValuePair)
}

func NewPersistentSliceKeyValuePair(contents []byte) (PersistentSlice, PersistentSlice) {
	return unmarshal(contents)
}

func (persistentSlice PersistentSlice) GetPersistentContents() []byte {
	return persistentSlice.contents
}

func (persistentSlice PersistentSlice) GetSlice() Slice {
	return NewSlice(persistentSlice.GetPersistentContents())
}

func (persistentSlice PersistentSlice) Size() int {
	return len(persistentSlice.contents)
}

func (persistentSlice *PersistentSlice) Add(other PersistentSlice) {
	persistentSlice.contents = append(persistentSlice.contents, other.contents...)
}

func ActualTotalSize(bytes []byte) uint32 {
	return bigEndian.Uint32(bytes)
}

func marshal(keyValuePair KeyValuePair) PersistentSlice {
	reservedTotalSize, reservedKeySize := ReservedTotalSize, ReservedKeySize
	actualTotalSize :=
		len(keyValuePair.Key.GetRawContent()) +
			len(keyValuePair.Value.GetRawContent()) +
			int(reservedKeySize) +
			int(reservedTotalSize)

	//The way PutCommand is encoded is: 4 bytes for totalSize | 4 bytes for keySize | Key content | Value content
	bytes := make([]byte, actualTotalSize)
	offset := 0

	bigEndian.PutUint32(bytes, uint32(actualTotalSize))
	offset = offset + int(reservedTotalSize)

	bigEndian.PutUint32(bytes[offset:], uint32(len(keyValuePair.Key.GetRawContent())))
	offset = offset + int(reservedKeySize)

	copy(bytes[offset:], keyValuePair.Key.GetRawContent())
	offset = offset + len(keyValuePair.Key.GetRawContent())

	copy(bytes[offset:], keyValuePair.Value.GetRawContent())
	return PersistentSlice{contents: bytes}
}

func unmarshal(bytes []byte) (PersistentSlice, PersistentSlice) {
	bytes = bytes[ReservedTotalSize:]
	keySize := bigEndian.Uint32(bytes)
	keyEndOffset := uint32(ReservedKeySize) + keySize

	return PersistentSlice{contents: bytes[ReservedKeySize:keyEndOffset]}, PersistentSlice{contents: bytes[keyEndOffset:]}
}
