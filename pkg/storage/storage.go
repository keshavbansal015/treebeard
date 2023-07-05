package storage

// How many levels exist in the storage tree
const LevelCount int = 32
const MaxAccessCount int = 8

func GetBlockOffset(level int, path int, storageID int, block string) (offset int, err error) {
	//TODO: implement
	return 0, nil
}

func GetAccessCount(level int, path int, storageID int) (count int, err error) {
	//TODO: implement
	return 0, nil
}

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of key to block values.
func ReadBucket(level int, path int, storageID int) (blocks map[string]string, err error) {
	//TODO: implement
	return map[string]string{
		"a": "storage_value_a",
		"b": "storage_value_b",
	}, nil
}

func WriteBucket(level int, path int, storageID int, stash map[string]string) (writtenBlocks map[string]string, err error) {
	// TODO: implement
	// TODO: It should make the counter zero
	writtenBlocks = make(map[string]string)
	for block, value := range stash {
		writtenBlocks[block] = value
	}
	return writtenBlocks, nil
}

func AtomicWriteBucket(level int, path int, storageID int, stash map[string]string) (writtenBlocks map[string]string, err error) {
	// TODO: implement
	// TODO: It should make the counter zero
	writtenBlocks = make(map[string]string)
	for block, value := range stash {
		writtenBlocks[block] = value
	}
	return writtenBlocks, nil
}

func ReadBlock(level int, path int, storageID int, offset int) (value string, err error) {
	// TODO: implement
	// TODO: it should invalidate and increase counter
	value = "test_read_block_from_storage"
	return value, nil
}
