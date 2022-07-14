package log

import (
	"sort"
	"storage-engine-workshop/db"
)

type WAL struct {
	directory       string
	activeSegment   *Segment
	passiveSegments []*Segment
}

func NewLog(directory string) (*WAL, error) {
	segment, err := NewSegment(directory, 0, 32)
	if err != nil {
		return nil, err
	}
	return &WAL{directory: directory, activeSegment: segment}, nil
}

func (log *WAL) Append(putCommand PutCommand) error {
	rollOverActiveSegment := func() error {
		//log.activeSegment.Close()
		log.passiveSegments = append(log.passiveSegments, log.activeSegment)
		if segment, err := NewSegment(log.directory, log.activeSegment.LastOffset(), 32); err != nil {
			return err
		} else {
			log.activeSegment = segment
		}
		return nil
	}
	appendToActiveSegment := func() error {
		if err := log.activeSegment.Append(db.NewPersistentSlice(db.KeyValuePair{Key: putCommand.key, Value: putCommand.value})); err != nil {
			return err
		}
		return nil
	}

	if log.activeSegment.IsMaxed() {
		if err := rollOverActiveSegment(); err != nil {
			return err
		}
	}
	return appendToActiveSegment()
}

func (log *WAL) ReadAll() ([]PutCommand, error) {
	sortSegments := func() []*Segment {
		allSegments := append(log.passiveSegments, log.activeSegment)
		sort.SliceStable(allSegments, func(i, j int) bool {
			return allSegments[i].baseOffSet < allSegments[j].baseOffSet
		})
		return allSegments
	}
	keyValuePairsToPutCommands := func(keyValuePairs []db.PersistentKeyValuePair) []PutCommand {
		var putCommands []PutCommand
		for _, pair := range keyValuePairs {
			putCommands = append(putCommands, NewPutCommand(pair.Key.GetSlice(), pair.Value.GetSlice()))
		}
		return putCommands
	}
	readAllSegments := func() ([]PutCommand, error) {
		var allPutCommands []PutCommand
		for _, segment := range sortSegments() {
			if keyValuePairs, err := segment.ReadAll(); err != nil {
				return nil, err
			} else {
				allPutCommands = append(allPutCommands, keyValuePairsToPutCommands(keyValuePairs)...)
			}
		}
		return allPutCommands, nil
	}
	return readAllSegments()
}
