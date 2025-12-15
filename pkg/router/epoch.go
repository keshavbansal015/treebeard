package router

import (
	"context"
	"math"
	"sync"
	"time"

	shardnodepb "github.com/keshavbansal015/treebeard/api/shardnode"
	"github.com/keshavbansal01515/treebeard/pkg/rpc"
	utils "github.com/keshavbansal01515/treebeard/pkg/utils"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

type epochManager struct {
	shardNodeRPCClients map[int]ReplicaRPCClientMap
	requests            map[int][]*request          // map of epoch round to requests
	reponseChans        map[int]map[string]chan any // map of epoch round to map of request id to response channel
	currentEpoch        int
	epochDuration       time.Duration
	hasher              utils.Hasher
	mu                  sync.Mutex
}

func newEpochManager(shardNodeRPCClients map[int]ReplicaRPCClientMap, epochDuration time.Duration) *epochManager {
	return &epochManager{
		shardNodeRPCClients: shardNodeRPCClients,
		requests:            make(map[int][]*request),
		reponseChans:        make(map[int]map[string]chan any),
		currentEpoch:        0,
		epochDuration:       epochDuration,
		hasher:              utils.Hasher{KnownHashes: make(map[string]uint32)},
	}
}

const (
	Read int = iota
	Write
)

type request struct {
	ctx           context.Context
	requestId     string
	operationType int
	block         string
	value         string
}

func (e *epochManager) addRequestToCurrentEpoch(r *request) chan any {
	log.Debug().Msgf("Aquiring lock for epoch manager in addRequestToCurrentEpoch")
	e.mu.Lock()
	log.Debug().Msgf("Aquired lock for epoch manager in addRequestToCurrentEpoch")
	log.Debug().Msgf("Adding request %v to epoch %d", r, e.currentEpoch)
	defer func() {
		log.Debug().Msgf("Releasing lock for epoch manager in addRequestToCurrentEpoch")
		e.mu.Unlock()
		log.Debug().Msgf("Released lock for epoch manager in addRequestToCurrentEpoch")
	}()
	e.requests[e.currentEpoch] = append(e.requests[e.currentEpoch], r)
	if _, exists := e.reponseChans[e.currentEpoch]; !exists {
		e.reponseChans[e.currentEpoch] = make(map[string]chan any)
	}
	e.reponseChans[e.currentEpoch][r.requestId] = make(chan any)
	return e.reponseChans[e.currentEpoch][r.requestId]
}

func (e *epochManager) whereToForward(block string) (shardNodeID int) {
	h := e.hasher.Hash(block)
	return int(math.Mod(float64(h), float64(len(e.shardNodeRPCClients))))
}

type readResponse struct {
	value string
	err   error
}

type writeResponse struct {
	success bool
	err     error
}

type batchResponse struct {
	readResponses  []*shardnodepb.ReadReply
	writeResponses []*shardnodepb.WriteReply
	err            error
}

func (e *epochManager) sendBatch(ctx context.Context, shardnodeClient ReplicaRPCClientMap, requestBatch *shardnodepb.RequestBatch, batchResponseChan chan batchResponse) {
	log.Debug().Msgf("Sending batch of %d requests %v to shardnode", len(requestBatch.ReadRequests)+len(requestBatch.WriteRequests), requestBatch)
	var replicaFuncs []rpc.CallFunc
	var clients []any
	for _, client := range shardnodeClient {
		replicaFuncs = append(replicaFuncs,
			func(ctx context.Context, client any, request any, opts ...grpc.CallOption) (any, error) {
				return client.(ShardNodeRPCClient).ClientAPI.BatchQuery(ctx, request.(*shardnodepb.RequestBatch), opts...)
			},
		)
		clients = append(clients, client)
	}
	reply, err := rpc.CallAllReplicas(ctx, clients, replicaFuncs, requestBatch)
	if err != nil {
		readReplies := make([]*shardnodepb.ReadReply, 0)
		writeReplies := make([]*shardnodepb.WriteReply, 0)
		for _, readRequest := range requestBatch.ReadRequests {
			readReplies = append(readReplies, &shardnodepb.ReadReply{RequestId: readRequest.RequestId, Value: ""})
		}
		for _, writeRequest := range requestBatch.WriteRequests {
			writeReplies = append(writeReplies, &shardnodepb.WriteReply{RequestId: writeRequest.RequestId, Success: false})
		}
		batchResponseChan <- batchResponse{err: err, readResponses: readReplies, writeResponses: writeReplies}
		return
	}
	log.Debug().Msgf("Received batch of requests from shardnode; reply: %v", reply)
	batchResponseChan <- batchResponse{readResponses: reply.(*shardnodepb.ReplyBatch).ReadReplies, writeResponses: reply.(*shardnodepb.ReplyBatch).WriteReplies, err: nil}
}

func (e *epochManager) getShardnodeBatches(requests []*request) map[int]*shardnodepb.RequestBatch {
	requestBatches := make(map[int]*shardnodepb.RequestBatch)
	for _, r := range requests {
		shardNodeID := e.whereToForward(r.block)
		if _, exists := requestBatches[shardNodeID]; !exists {
			requestBatches[shardNodeID] = &shardnodepb.RequestBatch{}
		}
		if r.operationType == Read {
			requestBatches[shardNodeID].ReadRequests = append(requestBatches[shardNodeID].ReadRequests, &shardnodepb.ReadRequest{RequestId: r.requestId, Block: r.block})
		} else {
			requestBatches[shardNodeID].WriteRequests = append(requestBatches[shardNodeID].WriteRequests, &shardnodepb.WriteRequest{RequestId: r.requestId, Block: r.block, Value: r.value})
		}
	}
	return requestBatches
}

// This function waits for all the responses then answers all of the requests.
// It can time out since a request may have failed.
func (e *epochManager) sendEpochRequestsAndAnswerThem(epochNumber int, requests []*request, responseChans map[string]chan any) {
	requestsCount := len(requests)
	if requestsCount == 0 {
		return
	}
	log.Debug().Msgf("Sending epoch requests and answering them for epoch %d with %d requests", epochNumber, requestsCount)
	batchRequests := e.getShardnodeBatches(requests)
	batchResponseChan := make(chan batchResponse)
	waitingCount := 0
	for shardNodeID, shardNodeRequests := range batchRequests {
		if len(shardNodeRequests.ReadRequests) == 0 && len(shardNodeRequests.WriteRequests) == 0 {
			continue
		}
		waitingCount++
		go e.sendBatch(context.Background(), e.shardNodeRPCClients[shardNodeID], shardNodeRequests, batchResponseChan)
	}
	timeout := time.After(10 * time.Second)
	for i := 0; i < waitingCount; i++ {
		select {
		case <-timeout:
			log.Error().Msgf("Timed out while waiting for batch response")
			return
		case reply := <-batchResponseChan:
			if reply.err != nil {
				log.Error().Msgf("Error while sending batch of requests; %s", reply.err)
				for _, r := range reply.readResponses {
					responseChans[r.RequestId] <- readResponse{err: reply.err}
				}
				for _, r := range reply.writeResponses {
					responseChans[r.RequestId] <- writeResponse{err: reply.err}
				}
				continue
			}
			log.Debug().Msgf("Received batch reply %v", reply)
			log.Debug().Msgf("Answering epoch requests for epoch %d", epochNumber)
			for _, r := range reply.readResponses {
				responseChans[r.RequestId] <- readResponse{value: r.Value}
			}
			for _, r := range reply.writeResponses {
				responseChans[r.RequestId] <- writeResponse{success: r.Success}
			}
		}
	}
}

// This function runs the epochManger forever.
func (e *epochManager) run() {
	for {
		epochTimeOut := time.After(e.epochDuration)
		<-epochTimeOut
		e.mu.Lock()
		e.currentEpoch++
		epochNumber := e.currentEpoch - 1
		go e.sendEpochRequestsAndAnswerThem(epochNumber, e.requests[epochNumber], e.reponseChans[epochNumber])
		e.mu.Unlock()
	}
}
