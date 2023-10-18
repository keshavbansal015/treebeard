package shardnode

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
)

type blockRequest struct {
	ctx   context.Context
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
	log.Debug().Msgf("Creating new batch manager with batch size %d", batchSize)
	batchManager := batchManager{}
	batchManager.batchSize = batchSize
	batchManager.storageQueues = make(map[int][]blockRequest)
	batchManager.responseChannel = make(map[string]chan string)
	return &batchManager
}

// It add the request to the correct queue and return a response channel.
// The client uses the response channel to get the result of this request.
func (b *batchManager) addRequestToStorageQueueAndWait(req blockRequest, storageID int) chan string {
	log.Debug().Msgf("Aquiring lock for batch manager in addRequestToStorageQueueAndWait")
	b.mu.Lock()
	log.Debug().Msgf("Aquired lock for batch manager in addRequestToStorageQueueAndWait")
	defer func() {
		log.Debug().Msgf("Releasing lock for batch manager in addRequestToStorageQueueAndWait")
		b.mu.Unlock()
		log.Debug().Msgf("Released lock for batch manager in addRequestToStorageQueueAndWait")
	}()

	b.storageQueues[storageID] = append(b.storageQueues[storageID], req)
	b.responseChannel[req.block] = make(chan string)
	return b.responseChannel[req.block]
}
