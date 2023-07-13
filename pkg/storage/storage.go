package storage

import "fmt"

// How many levels exist in the storage tree
const LevelCount int = 32
const MaxAccessCount int = 8

type StorageHandler struct {
	levelCount     int
	maxAccessCount int
}

func NewStorageHandler() *StorageHandler {
	return &StorageHandler{levelCount: LevelCount, maxAccessCount: MaxAccessCount}
}

func (s *StorageHandler) GetRandomPathAndStorageID() (path int, storageID int) {
	// TODO: implement
	return 0, 0
}

func (s *StorageHandler) GetBlockOffset(level int, path int, storageID int, block string) (offset int, err error) {
	// TODO: implement
	return 0, nil
}

func (s *StorageHandler) GetAccessCount(level int, path int, storageID int) (count int, err error) {
	// TODO: implement
	return 0, nil
}

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of key to block values.
func (s *StorageHandler) ReadBucket(level int, path int, storageID int) (blocks map[string]string, err error) {
	// TODO: implement
	return map[string]string{
		"a": "storage_value_a",
		"b": "storage_value_b",
	}, nil
}

func (s *StorageHandler) WriteBucket(level int, path int, storageID int, stash map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error) {
	// TODO: implement
	// TODO: It should make the counter zero
	writtenBlocks = make(map[string]string)
	for block, value := range stash {
		writtenBlocks[block] = value
	}
	return writtenBlocks, nil
}

func (s *StorageHandler) ReadBlock(level int, path int, storageID int, offset int) (value string, err error) {
	// TODO: implement
	// TODO: it should invalidate and increase counter
	value = "test_read_block_from_storage"
	return value, nil
}

func (s *StorageHandler) EarlyReshuffle(path int, storageID int) error {
	for level := 0; level < LevelCount; level++ {
		accessCount, err := s.GetAccessCount(level, path, storageID)
		if err != nil {
			return fmt.Errorf("unable to get access count from the server; %s", err)
		}
		if accessCount < MaxAccessCount {
			continue
		}
		localStash, err := s.ReadBucket(level, path, storageID)
		if err != nil {
			return fmt.Errorf("unable to read bucket from the server; %s", err)
		}
		writtenBlocks, err := s.WriteBucket(level, path, storageID, localStash, false)
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
