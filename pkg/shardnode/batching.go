package shardnode

import (
	"context"
	"time"

	"github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/rs/zerolog/log"
)

type blockRequest struct {
	ctx   context.Context
	block string
	path  int
}

type batchManager struct {
	batchTimeout    time.Duration
	storageQueues   map[int][]blockRequest // map of storage id to its requests
	responseChannel map[string]chan string // map of block to its response channel
	mu              utils.PriorityLock
}

func newBatchManager(batchTimeout time.Duration) *batchManager {
	log.Debug().Msgf("Creating new batch manager with batch timout %v", batchTimeout)
	batchManager := batchManager{}
	batchManager.batchTimeout = batchTimeout
	batchManager.storageQueues = make(map[int][]blockRequest)
	batchManager.responseChannel = make(map[string]chan string)
	batchManager.mu = utils.NewPriorityPreferenceLock()
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

type batchResponse struct {
	*oramnode.ReadPathReply
	err error
}

func (b *batchManager) asyncBatchRequests(ctx context.Context, storageID int, requests []blockRequest, oramNodeReplicaMap ReplicaRPCClientMap, responseChan chan batchResponse) {
	log.Debug().Msgf("Sending batch of requests to storageID %d with size %d", storageID, len(requests))
	reply, err := oramNodeReplicaMap.readPathFromAllOramNodeReplicas(context.Background(), requests, storageID)
	responseChan <- batchResponse{reply, err}
}
