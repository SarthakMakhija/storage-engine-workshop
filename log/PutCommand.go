package log

import (
	"storage-engine-workshop/db"
)

type PutCommand struct {
	keyValuePair db.KeyValuePair
}

func NewPutCommand(pair db.KeyValuePair) PutCommand {
	return PutCommand{
		keyValuePair: pair,
	}
}

func NewPutCommandWithKeyValue(key db.Slice, value db.Slice) PutCommand {
	return NewPutCommand(db.KeyValuePair{Key: key, Value: value})
}

func (putCommand PutCommand) key() db.Slice {
	return putCommand.keyValuePair.Key
}

func (putCommand PutCommand) value() db.Slice {
	return putCommand.keyValuePair.Value
}
