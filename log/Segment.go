package log

import (
	"fmt"
	"path"
	"storage-engine-workshop/db"
)

type Segment struct {
	directory    string
	store        *Store
	baseOffSet   int64
	maxSizeBytes uint64
}

func NewSegment(directory string, baseOffset int64, maxSizeBytes uint64) (*Segment, error) {
	store, err := NewStore(path.Join(directory, fmt.Sprintf("%s%d%s", "WAL_", baseOffset, ".store")))
	if err != nil {
		return nil, err
	}
	return &Segment{
		directory:    directory,
		store:        store,
		baseOffSet:   baseOffset,
		maxSizeBytes: maxSizeBytes,
	}, nil
}

func (segment *Segment) Append(persistentSlice db.PersistentSlice) error {
	err := segment.store.Append(persistentSlice)
	if err != nil {
		return err
	}
	return nil
}

func (segment *Segment) ReadAll() ([]db.PersistentKeyValuePair, error) {
	return segment.store.ReadAll()
}

func (segment *Segment) IsMaxed() bool {
	if segment.store.Size() >= int64(segment.maxSizeBytes) {
		return true
	}
	return false
}

func (segment *Segment) LastOffset() int64 {
	return int64(segment.store.Size()) + segment.baseOffSet
}

func (segment *Segment) Close() {
	segment.store.Close()
}
