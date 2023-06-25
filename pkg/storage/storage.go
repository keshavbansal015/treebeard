package storage

// How many levels exist in the storage tree
const LevelCount int = 32

func GetBlockOffset(level int, path int, block string) (offset int, err error) {
	//TODO: implement
	return 0, nil
}

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of key to block values.
func ReadBucket(level int, path int) (blocks map[string]string, err error) {
	//TODO: implement
	return map[string]string{
		"a": "storage_value_a",
		"b": "storage_value_b",
	}, nil
}

func WriteBucket(level int, path int, stash map[string]string) (writtenBlocks []string, err error) {
	// TODO: implement
	for block := range stash {
		writtenBlocks = append(writtenBlocks, block)
	}
	return writtenBlocks, nil
}
