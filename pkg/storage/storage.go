package storage

import "fmt"

// How many levels exist in the storage tree
const LevelCount int = 32
const MaxAccessCount int = 8

func GetRandomPathAndStorageID() (path int, storageID int) {
	// TODO: implement
	return 0, 0
}

func GetBlockOffset(level int, path int, storageID int, block string) (offset int, err error) {
	// TODO: implement
	return 0, nil
}

func GetAccessCount(level int, path int, storageID int) (count int, err error) {
	// TODO: implement
	return 0, nil
}

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of key to block values.
func ReadBucket(level int, path int, storageID int) (blocks map[string]string, err error) {
	// TODO: implement
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

func EarlyReshuffle(path int, storageID int) error {
	for level := 0; level < LevelCount; level++ {
		accessCount, err := GetAccessCount(level, path, storageID)
		if err != nil {
			return fmt.Errorf("unable to get access count from the server; %s", err)
		}
		if accessCount < MaxAccessCount {
			continue
		}
		localStash, err := ReadBucket(level, path, storageID)
		if err != nil {
			return fmt.Errorf("unable to read bucket from the server; %s", err)
		}
		writtenBlocks, err := WriteBucket(level, path, storageID, localStash)
		if err != nil {
			return fmt.Errorf("unable to write bucket from the server; %s", err)
		}
		for block := range localStash {
			if _, exists := writtenBlocks[block]; !exists {
				return fmt.Errorf("unable to write all blocks to the bucket")
			}
		}
	}
	return nil
}
