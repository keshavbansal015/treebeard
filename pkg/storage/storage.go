package storage

// TODO: It might need to handle multiple storage shards.

// MaxAccessCount is the maximum times we can access a bucket safely.
const MaxAccessCount int = 8

// StorageHandler is responsible for handling one or multiple storage shards.
type StorageHandler struct {
	maxAccessCount int
}

func NewStorageHandler() *StorageHandler {
	return &StorageHandler{maxAccessCount: MaxAccessCount}
}

func (s *StorageHandler) GetMaxAccessCount() int {
	return s.maxAccessCount
}

// It returns valid randomly chosen path and storageID.
func (s *StorageHandler) GetRandomPathAndStorageID() (path int, storageID int) {
	// TODO: implement
	return 0, 0
}

// It returns a block offset based on the blocks argument.
//
// If a real block is found, it returns isReal=true and the block id.
// If non of the "blocks" are in the bucket, it returns isReal=false
func (s *StorageHandler) GetBlockOffset(bucketID int, storageID int, blocks []string) (offset int, isReal bool, blockFound string, err error) {
	// TODO: implement
	return 0, true, "a", nil
}

// It returns the number of times a bucket was accessed.
// This is helpful to know when to do an early reshuffle.
func (s *StorageHandler) GetAccessCount(bucketID int, storageID int) (count int, err error) {
	// TODO: implement
	return 0, nil
}

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of block id to block values.
func (s *StorageHandler) ReadBucket(bucketID int, storageID int) (blocks map[string]string, err error) {
	// TODO: implement
	return map[string]string{
		"a": "storage_value_a",
		"b": "storage_value_b",
	}, nil
}

// WriteBucket writes readBucketBlocks and shardNodeBlocks to the storage shard.
// It priorotizes readBucketBlocks to shardNodeBlocks.
// It returns the blocks that were written into the storage shard in the writtenBlocks variable.
func (s *StorageHandler) WriteBucket(bucketID int, storageID int, readBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error) {
	// TODO: implement
	// TODO: It should make the counter zero
	writtenBlocks = make(map[string]string)
	for block, value := range readBucketBlocks {
		writtenBlocks[block] = value
	}
	for block, value := range shardNodeBlocks {
		writtenBlocks[block] = value
	}
	return writtenBlocks, nil
}

// ReadBlock reads a single block using an its offset.
func (s *StorageHandler) ReadBlock(bucketID int, storageID int, offset int) (value string, err error) {
	// TODO: implement
	// TODO: it should invalidate and increase counter
	value = "test_read_block_from_storage"
	return value, nil
}

// GetBucketsInPaths return all the bucket ids for the passed paths.
func (s *StorageHandler) GetBucketsInPaths(paths []int) (bucketIDs []int, err error) {
	// TODO: implement
	return []int{0, 1, 2}, nil
}
