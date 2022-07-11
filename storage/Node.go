package storage

import (
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

func (node *Node) Get(key db.Slice, keyComparator comparator.KeyComparator) (db.Slice, bool) {
	current := node
	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil &&
			keyComparator.Compare(current.forwards[level].key, key) < 0 {
			current = current.forwards[level]
		}
	}
	current = current.forwards[0]
	if current != nil && keyComparator.Compare(current.key, key) == 0 {
		return current.value, true
	}
	return db.NilSlice(), false
}
