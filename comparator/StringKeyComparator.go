package comparator

import (
	"storage-engine-workshop/db"
	"strings"
)

type StringKeyComparator struct {
}

func (comparator StringKeyComparator) Compare(one db.Slice, other db.Slice) int {
	return strings.Compare(string(one.GetRawContent()), string(other.GetRawContent()))
}
