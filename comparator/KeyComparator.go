package comparator

import (
	"storage-engine-workshop/db"
)

type KeyComparator interface {
	Compare(one db.Slice, other db.Slice) int
}
