package log

import (
	"encoding/binary"
	"storage-engine-workshop/db"
	"unsafe"
)

var (
	BigEndian         = binary.BigEndian
	ReservedKeySize   = unsafe.Sizeof(uint32(0))
	ReservedValueSize = unsafe.Sizeof(uint32(0))
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

func NewPersistentSliceUsingRaw(contents []byte) PersistentSlice {
	return PersistentSlice{Contents: contents}
}

func (persistentSlice PersistentSlice) GetPersistentContents() []byte {
	return persistentSlice.Contents
}

func (persistentSlice PersistentSlice) GetSlice() db.Slice {
	return db.NewSlice(persistentSlice.GetPersistentContents())
}

func ActualKeySize(bytes []byte) uint32 {
	return BigEndian.Uint32(bytes)
}

func ActualValueSize(bytes []byte) uint32 {
	return BigEndian.Uint32(bytes)
}

func marshal(putCommand PutCommand) PersistentSlice {
	keySize, valueSize := ReservedKeySize, ReservedValueSize
	bytes := make([]byte, len(putCommand.key.GetRawContent())+len(putCommand.value.GetRawContent())+int(keySize)+int(valueSize))
	offset := 0

	BigEndian.PutUint32(bytes, uint32(len(putCommand.key.GetRawContent())))
	offset = offset + int(keySize)

	BigEndian.PutUint32(bytes[offset:], uint32(len(putCommand.value.GetRawContent())))
	offset = offset + int(valueSize)

	copy(bytes[offset:], putCommand.key.GetRawContent())
	offset = offset + len(putCommand.key.GetRawContent())

	copy(bytes[offset:], putCommand.value.GetRawContent())
	return PersistentSlice{Contents: bytes}
}
