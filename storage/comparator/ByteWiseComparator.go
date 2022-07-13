package comparator

import (
	"bytes"
	"storage-engine-workshop/db"
)

type ByteWiseComparator struct {
}

func (comparator ByteWiseComparator) Compare(one db.Slice, other db.Slice) int {
	return bytes.Compare(one.GetRawContent(), other.GetRawContent())
}
