package shardnode

import "sync"

type blockRequest struct {
	block string
	path  int
}

type batchManager struct {
	batchSize       int
	storageQueues   map[int][]blockRequest // map of storage id to its requests
	responseChannel map[string]chan string
	mu              sync.Mutex
}

func newBatchManager(batchSize int) *batchManager {
	batchManager := batchManager{}
	batchManager.batchSize = batchSize
	batchManager.storageQueues = make(map[int][]blockRequest)
	batchManager.responseChannel = make(map[string]chan string)
	return &batchManager
}

// It add the request to the correct queue and return a response channel.
// The client uses the response channel to get the result of this request.
func (b *batchManager) addRequestToStorageQueueAndWait(req blockRequest, storageID int) chan string {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.storageQueues[storageID] = append(b.storageQueues[storageID], req)
	b.responseChannel[req.block] = make(chan string)
	return b.responseChannel[req.block]
}

// It simply adds the request to the correct queue.
func (b *batchManager) addRequestToStorageQueueWithoutWaiting(req blockRequest, storageID int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.storageQueues[storageID] = append(b.storageQueues[storageID], req)
}
