# Idea

This repository will be used in "building storage engine" workshop. As a part of that workshop, we will build a storage engine using LSM tree, but we will not implement
merge and compaction.

This repository is supposed to contain the following:
1. Implementation of Memtable
2. Implementation of SSTable and write to disk
3. Implementation of concurrency, either a single write/multiple readers pattern or a singular update queue
4. Implementation of write-ahead logging
5. Implementation of transactions
6. Implementation of Bloom filter
7. Implementation of "Put", "Get" and "Update"
8. Implementation of "Get" in Memtable and SSTable
9. Implementation of "Update" using versioned put

# Build Status
[![Actions Status](https://github.com/SarthakMakhija/storage-engine-workshop/workflows/GoCI/badge.svg)](https://github.com/SarthakMakhija/storage-engine-workshop/actions)

# Running the tests
`go test -v ./...`