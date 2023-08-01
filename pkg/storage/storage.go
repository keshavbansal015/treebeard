package storage

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

func (s *StorageHandler) GetLevelCount() int {
	return s.levelCount
}

func (s *StorageHandler) GetMaxAccessCount() int {
	return s.maxAccessCount
}

func (s *StorageHandler) GetRandomPathAndStorageID() (path int, storageID int) {
	// TODO: implement
	return 0, 0
}

func (s *StorageHandler) GetBlockOffset(bucketID int, storageID int, blocks []string) (offset int, isReal bool, blockFound string, err error) {
	// TODO: implement
	return 0, true, "a", nil
}

func (s *StorageHandler) GetAccessCount(bucketID int, storageID int) (count int, err error) {
	// TODO: implement
	return 0, nil
}

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of key to block values.
func (s *StorageHandler) ReadBucket(bucketID int, storageID int) (blocks map[string]string, err error) {
	// TODO: implement
	return map[string]string{
		"a": "storage_value_a",
		"b": "storage_value_b",
	}, nil
}

// TODO: think about the following scenario
// There is a block that came from both shardNodeBlocks and ReadBucketBlocks
// First, is this possible?
// Second, what should we do?

// TODO: WriteBucket SHOULD first write the ReadBucketBlocks.
func (s *StorageHandler) WriteBucket(bucketID int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error) {
	// TODO: implement
	// TODO: It should make the counter zero
	writtenBlocks = make(map[string]string)
	for block, value := range ReadBucketBlocks {
		writtenBlocks[block] = value
	}
	for block, value := range shardNodeBlocks {
		writtenBlocks[block] = value
	}
	return writtenBlocks, nil
}

func (s *StorageHandler) ReadBlock(bucketID int, storageID int, offset int) (value string, err error) {
	// TODO: implement
	// TODO: it should invalidate and increase counter
	value = "test_read_block_from_storage"
	return value, nil
}

func (s *StorageHandler) GetBucketsInPaths(paths []int) (bucketIDs []int, err error) {
	// TODO: implement
	return []int{0, 1, 2}, nil
}
