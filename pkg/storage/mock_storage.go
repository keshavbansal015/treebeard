package storage

type MockStorageHandler struct {
	levelCount                int
	maxAccessCount            int
	customBatchGetBlockOffset func(bucketIDs []int, storageID int, blocks []string) (offsets map[int]BlockOffsetStatus, err error)
	customBatchGetAccessCount func(bucketIDs []int, storageID int) (counts map[int]int, err error)
	customBatchReadBucket     func(bucketIDs []int, storageID int) (blocks map[int]map[string]string, err error)
	customBatchWriteBucket    func(storageID int, readBucketBlocksList map[int]map[string]string, shardNodeBlocks map[string]BlockInfo) (writtenBlocks map[string]string, err error)
	customBatchReadBlock      func(offsets map[int]int, storageID int) (values map[int]string, err error)
}

func NewMockStorageHandler(levelCount int, maxAccessCount int) *MockStorageHandler {
	return &MockStorageHandler{
		levelCount:     levelCount,
		maxAccessCount: maxAccessCount,
		customBatchGetBlockOffset: func(bucketIDs []int, storageID int, blocks []string) (offsets map[int]BlockOffsetStatus, err error) {
			return nil, nil
		},
		customBatchGetAccessCount: func(bucketIDs []int, storageID int) (counts map[int]int, err error) {
			return nil, nil
		},
		customBatchReadBucket: func(bucketIDs []int, storageID int) (blocks map[int]map[string]string, err error) {
			return nil, nil
		},
		customBatchWriteBucket: func(storageID int, readBucketBlocksList map[int]map[string]string, shardNodeBlocks map[string]BlockInfo) (writtenBlocks map[string]string, err error) {
			return nil, nil
		},
		customBatchReadBlock: func(offsets map[int]int, storageID int) (values map[int]string, err error) {
			return nil, nil
		},
	}
}

func (m *MockStorageHandler) GetMaxAccessCount() int {
	return m.maxAccessCount
}

func (m *MockStorageHandler) LockStorage(storageID int) {
}

func (m *MockStorageHandler) UnlockStorage(storageID int) {
}

func (m *MockStorageHandler) BatchGetBlockOffset(bucketIDs []int, storageID int, blocks []string) (offsets map[int]BlockOffsetStatus, err error) {
	return m.customBatchGetBlockOffset(bucketIDs, storageID, blocks)
}

func (m *MockStorageHandler) WithCusomBatchGetBlockOffsetFunc(f func(bucketIDs []int, storageID int, blocks []string) (offsets map[int]BlockOffsetStatus, err error)) *MockStorageHandler {
	m.customBatchGetBlockOffset = f
	return m
}

func (m *MockStorageHandler) BatchGetAccessCount(bucketIDs []int, storageID int) (counts map[int]int, err error) {
	return m.customBatchGetAccessCount(bucketIDs, storageID)
}

func (m *MockStorageHandler) WithCustomBatchGetAccessCountFunc(f func(bucketIDs []int, storageID int) (counts map[int]int, err error)) *MockStorageHandler {
	m.customBatchGetAccessCount = f
	return m
}

func (m *MockStorageHandler) BatchReadBucket(bucketIDs []int, storageID int) (blocks map[int]map[string]string, err error) {
	return m.customBatchReadBucket(bucketIDs, storageID)
}

func (m *MockStorageHandler) WithCustomBatchReadBucketFunc(f func(bucketIDs []int, storageID int) (blocks map[int]map[string]string, err error)) *MockStorageHandler {
	m.customBatchReadBucket = f
	return m
}

func (m *MockStorageHandler) BatchWriteBucket(storageID int, readBucketBlocksList map[int]map[string]string, shardNodeBlocks map[string]BlockInfo) (writtenBlocks map[string]string, err error) {
	return m.customBatchWriteBucket(storageID, readBucketBlocksList, shardNodeBlocks)
}

func (m *MockStorageHandler) WithCustomBatchWriteBucketFunc(f func(storageID int, readBucketBlocksList map[int]map[string]string, shardNodeBlocks map[string]BlockInfo) (writtenBlocks map[string]string, err error)) *MockStorageHandler {
	m.customBatchWriteBucket = f
	return m
}

func (m *MockStorageHandler) BatchReadBlock(offsets map[int]int, storageID int) (values map[int]string, err error) {
	return m.customBatchReadBlock(offsets, storageID)
}

func (m *MockStorageHandler) WithCustomBatchReadBlockFunc(f func(offsets map[int]int, storageID int) (values map[int]string, err error)) *MockStorageHandler {
	m.customBatchReadBlock = f
	return m
}

func (m *MockStorageHandler) GetBucketsInPaths(paths []int) (bucketIDs []int, err error) {
	return []int{1, 2, 3, 4}, nil
}

func (m *MockStorageHandler) GetMultipleReverseLexicographicPaths(evictionCount int, count int) (paths []int) {
	for i := 0; i < count; i++ {
		paths = append(paths, 1)
	}
	return paths
}

func (m *MockStorageHandler) GetRandomStorageID() int {
	return 0
}
