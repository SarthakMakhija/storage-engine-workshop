package storage

import (
	"sort"
	"storage-engine-workshop/comparator"
	"storage-engine-workshop/db"
	"storage-engine-workshop/utils"
)

type Node struct {
	key      db.Slice
	value    db.Slice
	forwards []*Node
}

func NewNode(key db.Slice, value db.Slice, level int) *Node {
	return &Node{
		key:      key,
		value:    value,
		forwards: make([]*Node, level),
	}
}

func (node *Node) Put(key db.Slice, value db.Slice, keyComparator comparator.KeyComparator, levelGenerator utils.LevelGenerator) bool {
	current := node
	positions := make([]*Node, len(node.forwards))

	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil &&
			keyComparator.Compare(current.forwards[level].key, key) < 0 {
			current = current.forwards[level]
		}
		positions[level] = current
	}

	current = current.forwards[0]
	if current == nil || keyComparator.Compare(current.key, key) != 0 {
		newLevel := levelGenerator.Generate()
		newNode := NewNode(key, value, newLevel)
		for level := 0; level < newLevel; level++ {
			newNode.forwards[level] = positions[level].forwards[level]
			positions[level].forwards[level] = newNode
		}
		return true
	}
	return false
}

func (node *Node) Get(key db.Slice, keyComparator comparator.KeyComparator) db.GetResult {
	node, ok := node.nodeMatching(key, keyComparator)
	if ok {
		return db.GetResult{Value: node.value, Exists: ok}
	}
	return db.GetResult{Value: db.NilSlice(), Exists: false}
}

func (node *Node) MultiGet(keys []db.Slice, keyComparator comparator.KeyComparator) db.MultiGetResult {
	sort.SliceStable(keys, func(i, j int) bool {
		return keyComparator.Compare(keys[i], keys[j]) < 0
	})
	currentNode := node
	response := db.MultiGetResult{}
	for _, key := range keys {
		targetNode, ok := currentNode.nodeMatching(key, keyComparator)
		if ok {
			response.Add(db.GetResult{Value: targetNode.value, Exists: ok})
			currentNode = targetNode
		} else {
			response.Add(db.GetResult{Value: db.NilSlice(), Exists: false})
		}
	}
	return response
}

func (node *Node) AggregatePersistentSlice() db.PersistentSlice {
	level, current := 0, node
	current = current.forwards[level]

	persistentSlice := db.EmptyPersistentSlice()
	for current != nil {
		slice := db.NewPersistentSlice(db.KeyValuePair{Key: current.key, Value: current.value})
		persistentSlice.Add(slice)
		current = current.forwards[level]
	}
	return persistentSlice
}

func (node *Node) nodeMatching(key db.Slice, keyComparator comparator.KeyComparator) (*Node, bool) {
	current := node
	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil &&
			keyComparator.Compare(current.forwards[level].key, key) < 0 {
			current = current.forwards[level]
		}
	}
	current = current.forwards[0]
	if current != nil && keyComparator.Compare(current.key, key) == 0 {
		return current, true
	}
	return nil, false
}
