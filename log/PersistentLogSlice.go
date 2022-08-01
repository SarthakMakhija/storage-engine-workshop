package log

import (
	"encoding/binary"
	"storage-engine-workshop/db/model"
	"unsafe"
)

var (
	bigEndian                     = binary.BigEndian
	reservedTotalSize             = unsafe.Sizeof(uint32(0))
	reservedKeySize               = unsafe.Sizeof(uint32(0))
	transactionMarkerSize         = 2
	transactionMarkerBegin   byte = 'B'
	transactionMarkerSuccess byte = 'S'
	transactionMarkerFailed  byte = 'F'
)

type TransactionStatus int

const (
	TransactionStatusSuccess  TransactionStatus = iota
	TransactionStatusFailed   TransactionStatus = 1
	TransactionStatusDangling TransactionStatus = 2
)

type PersistentLogSlice struct {
	contents []byte
}

var emptyPersistentLogSlice = PersistentLogSlice{contents: []byte{}}

func EmptyPersistentLogSlice() PersistentLogSlice {
	return emptyPersistentLogSlice
}

func NewPersistentLogSlice(keyValuePair model.KeyValuePair) PersistentLogSlice {
	return marshal(keyValuePair)
}

func NewPersistentLogSliceKeyValuePair(contents []byte) (PersistentLogSlice, PersistentLogSlice, TransactionStatus) {
	return unmarshal(contents)
}

func (persistentLogSlice PersistentLogSlice) GetPersistentContents() []byte {
	return persistentLogSlice.contents
}

func (persistentLogSlice PersistentLogSlice) GetSlice() model.Slice {
	return model.NewSlice(persistentLogSlice.GetPersistentContents())
}

func (persistentLogSlice PersistentLogSlice) Size() int {
	return len(persistentLogSlice.contents)
}

func (persistentLogSlice *PersistentLogSlice) Add(other PersistentLogSlice) {
	persistentLogSlice.contents = append(persistentLogSlice.contents, other.contents...)
}

func ActualTotalSize(bytes []byte) uint32 {
	return bigEndian.Uint32(bytes)
}

func marshal(keyValuePair model.KeyValuePair) PersistentLogSlice {
	reservedTotalSize, reservedKeySize := reservedTotalSize, reservedKeySize
	actualTotalSize :=
		len(keyValuePair.Key.GetRawContent()) +
			len(keyValuePair.Value.GetRawContent()) +
			int(reservedKeySize) +
			int(reservedTotalSize) + transactionMarkerSize

	//The way PutCommand is encoded is: 4 bytes for totalSize | 4 bytes for keySize | Key content | Value content | 1 byte for transaction marker begin | 1 byte for transaction marker end |
	bytes := make([]byte, actualTotalSize)
	offset := 0

	bigEndian.PutUint32(bytes, uint32(actualTotalSize))
	offset = offset + int(reservedTotalSize)

	bigEndian.PutUint32(bytes[offset:], uint32(len(keyValuePair.Key.GetRawContent())))
	offset = offset + int(reservedKeySize)

	copy(bytes[offset:], keyValuePair.Key.GetRawContent())
	offset = offset + len(keyValuePair.Key.GetRawContent())

	copy(bytes[offset:], keyValuePair.Value.GetRawContent())
	offset = offset + len(keyValuePair.Value.GetRawContent())

	bytes[offset] = transactionMarkerBegin
	return PersistentLogSlice{contents: bytes}
}

func unmarshal(bytes []byte) (PersistentLogSlice, PersistentLogSlice, TransactionStatus) {
	bytes = bytes[reservedTotalSize:]
	size := len(bytes)
	keySize := bigEndian.Uint32(bytes)
	keyEndOffset := uint32(reservedKeySize) + keySize
	valueEndOffset := size - 2

	key, value := PersistentLogSlice{contents: bytes[reservedKeySize:keyEndOffset]}, PersistentLogSlice{contents: bytes[keyEndOffset:valueEndOffset]}
	switch bytes[size-1] {
	case transactionMarkerSuccess:
		return key, value, TransactionStatusSuccess
	case transactionMarkerFailed:
		return key, value, TransactionStatusFailed
	default:
		return key, value, TransactionStatusDangling
	}
}
