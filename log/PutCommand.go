package log

import (
	"storage-engine-workshop/db"
)

type PutCommand struct {
	key   db.Slice
	value db.Slice
}

func NewPutCommand(key, value db.Slice) PutCommand {
	return PutCommand{
		key:   key,
		value: value,
	}
}
