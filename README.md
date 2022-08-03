# Idea

This repository will be used in "building storage engine" workshop. As a part of that workshop, we will build a storage engine using LSM tree, but we will not implement
merge and compaction.

This repository is supposed to contain the following:
- [X] Implementation of Memtable
- [X] Implementation of SSTable
- [X] Implementation of concurrency, using either a single writer/multiple readers pattern or a singular update queue pattern
- [X] Implementation of write-ahead logging
- [X] Implementation of transactions
- [X] Implementation of Bloom filter
- [X] Implementation of "Put", "Get", "MultiGet"
- [X] Implementation of "Get" and "MultiGet" in Memtable and SSTable
- [ ] Implementation of "Update" using versioned put (Next version)

# Build Status
[![Actions Status](https://github.com/SarthakMakhija/storage-engine-workshop/workflows/GoCI/badge.svg)](https://github.com/SarthakMakhija/storage-engine-workshop/actions)

# Running the tests
`go test -v ./...`
