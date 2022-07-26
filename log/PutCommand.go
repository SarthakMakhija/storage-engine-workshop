package log

import (
	"storage-engine-workshop/db/model"
)

type PutCommand struct {
	keyValuePair model.KeyValuePair
}

func NewPutCommand(pair model.KeyValuePair) PutCommand {
	return PutCommand{
		keyValuePair: pair,
	}
}

func NewPutCommandWithKeyValue(key model.Slice, value model.Slice) PutCommand {
	return NewPutCommand(model.KeyValuePair{Key: key, Value: value})
}

func (putCommand PutCommand) key() model.Slice {
	return putCommand.keyValuePair.Key
}

func (putCommand PutCommand) value() model.Slice {
	return putCommand.keyValuePair.Value
}
