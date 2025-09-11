package oramnode

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/keshavbansal015/treebeard/api/oramnode"
	"github.com/keshavbansal015/treebeard/pkg/commonerrs"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/rpc"
	strg "github.com/keshavbansal015/treebeard/pkg/storage"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type storage interface {
	GetMaxAccessCount() int
	LockStorage(storageID int)
	UnlockStorage(storageID int)
	BatchGetBlockOffset(bucketIDs []int, storageID int, blocks []string) (offsets map[int]strg.BlockOffsetStatus, err error)
	BatchGetAccessCount(bucketIDs []int, storageID int) (counts map[int]int, err error)
	BatchReadBucket(bucketIDs []int, storageID int) (blocks map[int]map[string]string, err error)
	BatchWriteBucket(storageID int, readBucketBlocksList map[int]map[string]string, shardNodeBlocks map[string]strg.BlockInfo) (writtenBlocks map[string]string, err error)
	BatchReadBlock(offsets map[int]int, storageID int) (values map[int]string, err error)
	GetBucketsInPaths(paths []int) (bucketIDs []int, err error)
	GetRandomStorageID() int
	GetMultipleReverseLexicographicPaths(evictionCount int, count int) (paths []int)
}

type oramNodeServer struct {
	pb.UnimplementedOramNodeServer
	oramNodeServerID    int
	replicaID           int
	raftNode            *raft.Raft
	oramNodeFSM         *oramNodeFSM
	shardNodeRPCClients ShardNodeRPCClients
	readPathCounter     atomic.Int32
	storageHandler      storage
	parameters          config.Parameters
}

func newOramNodeServer(oramNodeServerID int, replicaID int, raftNode *raft.Raft, oramNodeFSM *oramNodeFSM, shardNodeRPCClients map[int]ReplicaRPCClientMap, storageHandler storage, parameters config.Parameters) *oramNodeServer {
	log.Debug().Msgf("Initializing ORAM Node Server with ID %d and Replica ID %d", oramNodeServerID, replicaID)
	return &oramNodeServer{
		oramNodeServerID:    oramNodeServerID,
		replicaID:           replicaID,
		raftNode:            raftNode,
		oramNodeFSM:         oramNodeFSM,
		shardNodeRPCClients: shardNodeRPCClients,
		readPathCounter:     atomic.Int32{},
		storageHandler:      storageHandler,
		parameters:          parameters,
	}
}

// It runs the failed eviction and read path as the new leader.
func (o *oramNodeServer) performFailedOperations() error {
	log.Debug().Msg("Waiting to become a leader to perform failed operations.")
	<-o.raftNode.LeaderCh()
	log.Debug().Msg("Node is now the leader. Checking for unfinished operations.")
	o.oramNodeFSM.unfinishedEvictionMu.Lock()
	o.oramNodeFSM.unfinishedReadPathMu.Lock()
	needsEviction := o.oramNodeFSM.unfinishedEviction
	needsReadPath := o.oramNodeFSM.unfinishedReadPath
	o.oramNodeFSM.unfinishedEvictionMu.Unlock()
	o.oramNodeFSM.unfinishedReadPathMu.Unlock()
	if needsEviction != nil {
		o.oramNodeFSM.unfinishedEvictionMu.Lock()
		storageID := o.oramNodeFSM.unfinishedEviction.storageID
		o.oramNodeFSM.unfinishedEvictionMu.Unlock()
		log.Debug().Msgf("Performing failed eviction for storageID %d", storageID)
		o.evict(storageID)
	}
	if needsReadPath != nil {
		log.Debug().Msgf("Performing failed read path")
		o.oramNodeFSM.unfinishedReadPathMu.Lock()
		paths := o.oramNodeFSM.unfinishedReadPath.paths
		storageID := o.oramNodeFSM.unfinishedReadPath.storageID
		o.oramNodeFSM.unfinishedReadPathMu.Unlock()
		buckets, _ := o.storageHandler.GetBucketsInPaths(paths)
		log.Debug().Msgf("Performing failed read path with paths %v and storageID %d", paths, storageID)
		o.earlyReshuffle(buckets, storageID)
	}
	return nil
}

type getAccessCountResponse struct {
	counts map[int]int
	err    error
}

func (o *oramNodeServer) asyncGetAccessCount(bucketIDs []int, storageID int, responseChan chan getAccessCountResponse) {
	log.Debug().Msgf("Async getting access count for buckets %v and storageID %d", bucketIDs, storageID)
	counts, err := o.storageHandler.BatchGetAccessCount(bucketIDs, storageID)
	log.Debug().Msgf("Received access counts %v for buckets %v", counts, bucketIDs)
	responseChan <- getAccessCountResponse{counts: counts, err: err}
}

func (o *oramNodeServer) earlyReshuffle(buckets []int, storageID int) error {
	log.Debug().Msgf("Starting early reshuffle for buckets %v and storageID %d", buckets, storageID)
	// TODO: can we make this a background thread?
	batches := distributeBucketIDs(buckets, o.parameters.RedisPipelineSize)
	accessCountChan := make(chan getAccessCountResponse)
	log.Debug().Msgf("Distributing %d buckets into %d batches for access count", len(buckets), len(batches))
	for _, bucketIDs := range batches {
		go o.asyncGetAccessCount(bucketIDs, storageID, accessCountChan)
	}
	var bucketsToWrite []int
	for i := 0; i < len(batches); i++ {
		response := <-accessCountChan
		if response.err != nil {
			log.Error().Msgf("Failed to get access count from the server: %s", response.err)
			return fmt.Errorf("unable to get access count from the server; %s", response.err)
		}
		for bucket, accessCount := range response.counts {
			log.Debug().Msgf("Bucket %d access count: %d, MaxAccessCount: %d", bucket, accessCount, o.storageHandler.GetMaxAccessCount())
			if accessCount >= o.storageHandler.GetMaxAccessCount() {
				bucketsToWrite = append(bucketsToWrite, bucket)
			}
		}
	}
	log.Debug().Msgf("Identified %d buckets to write back to: %v", len(bucketsToWrite), bucketsToWrite)
	readBucketChan := make(chan readBucketResponse)
	batches = distributeBucketIDs(bucketsToWrite, o.parameters.RedisPipelineSize)
	log.Debug().Msgf("Distributing %d buckets into %d batches for reading", len(bucketsToWrite), len(batches))
	for _, bucketIDs := range batches {
		go o.asyncReadBucket(bucketIDs, storageID, readBucketChan)
	}
	blocksFromReadBucketBatches := make([]map[int]map[string]string, len(batches))
	for i := 0; i < len(batches); i++ {
		response := <-readBucketChan
		if response.err != nil {
			log.Error().Msgf("Failed to read bucket from the server: %s", response.err)
			return fmt.Errorf("unable to read bucket from the server; %s", response.err)
		}
		blocksFromReadBucketBatches[i] = response.bucketValues
	}
	log.Debug().Msgf("Read blocks from all %d batches. Now writing back.", len(batches))
	for _, blocks := range blocksFromReadBucketBatches {
		log.Debug().Msgf("Batch writing %d blocks to storage %d", len(blocks), storageID)
		go o.storageHandler.BatchWriteBucket(storageID, blocks, nil)
	}
	return nil
}

type readBucketResponse struct {
	bucketValues map[int]map[string]string
	err          error
}

func (o *oramNodeServer) asyncReadBucket(bucketIDs []int, storageID int, responseChan chan readBucketResponse) {
	log.Debug().Msgf("Async reading buckets %v with storageID %d", bucketIDs, storageID)
	bucketValues, err := o.storageHandler.BatchReadBucket(bucketIDs, storageID)
	log.Debug().Msgf("Finished async read for buckets %v", bucketIDs)
	responseChan <- readBucketResponse{bucketValues: bucketValues, err: err}
}

func (o *oramNodeServer) readAllBuckets(buckets []int, storageID int) (blocksFromReadBucket map[int]map[string]string, err error) {
	log.Debug().Msgf("Reading all buckets with buckets %v and storageID %d", buckets, storageID)
	blocksFromReadBucket = make(map[int]map[string]string) // map of bucket to map of block to value
	for _, bucket := range buckets {
		blocksFromReadBucket[bucket] = make(map[string]string)
	}
	if err != nil {
		log.Error().Msgf("Failed to get bucket ids for early reshuffle path: %v", err)
		return nil, fmt.Errorf("unable to get bucket ids for early reshuffle path; %v", err)
	}
	for _, bucket := range buckets {
		blocksFromReadBucket[bucket] = make(map[string]string)
	}
	readBucketResponseChan := make(chan readBucketResponse)
	batches := distributeBucketIDs(buckets, o.parameters.RedisPipelineSize)
	log.Debug().Msgf("Distributing %d buckets into %d batches for readAllBuckets", len(buckets), len(batches))
	for _, bucketIDs := range batches {
		go o.asyncReadBucket(bucketIDs, storageID, readBucketResponseChan)
	}

	for i := 0; i < len(batches); i++ {
		response := <-readBucketResponseChan
		log.Debug().Msgf("Got blocks from asyncReadBucket: %v", response.bucketValues)
		if response.err != nil {
			log.Error().Msgf("Failed to read bucket: %s", err)
			return nil, fmt.Errorf("unable to read bucket; %s", err)
		}
		for bucket, blockValues := range response.bucketValues {
			for block, value := range blockValues {
				blocksFromReadBucket[bucket][block] = value
			}
		}
	}
	log.Debug().Msgf("Finished reading all buckets. Total buckets read: %d", len(blocksFromReadBucket))
	return blocksFromReadBucket, nil
}

func (o *oramNodeServer) readBlocksFromShardNode(paths []int, storageID int, randomShardNode ReplicaRPCClientMap) (receivedBlocks map[string]strg.BlockInfo, err error) {
	log.Debug().Msgf("Reading blocks from shard node with paths %v and storageID %d", paths, storageID)
	receivedBlocks = make(map[string]strg.BlockInfo) // map of received block to value and path

	log.Debug().Msgf("Requesting %d blocks from shard node for storageID %d", o.parameters.MaxBlocksToSend, storageID)
	shardNodeBlocks, err := randomShardNode.getBlocksFromShardNode(storageID, o.parameters.MaxBlocksToSend)
	if err != nil {
		log.Error().Msgf("Failed to get blocks from shard node: %s", err)
		return nil, fmt.Errorf("unable to get blocks from shard node; %s", err)
	}
	log.Debug().Msgf("Received %d blocks from shard node", len(shardNodeBlocks))
	for _, block := range shardNodeBlocks {
		receivedBlocks[block.Block] = strg.BlockInfo{Value: block.Value, Path: int(block.Path)}
		log.Trace().Msgf("Received block: %s, path: %d", block.Block, block.Path)
	}
	return receivedBlocks, nil
}

func (o *oramNodeServer) writeBackBlocksToAllBuckets(buckets []int, storageID int, blocksFromReadBucket map[int]map[string]string, receivedBlocks map[string]strg.BlockInfo) (receivedBlocksIsWritten map[string]bool, err error) {
	log.Debug().Msgf("Writing back blocks to all buckets for storageID %d", storageID)
	log.Debug().Msgf("Blocks from read bucket: %v", blocksFromReadBucket)
	log.Debug().Msgf("Received blocks to write: %v", receivedBlocks)
	receivedBlocksIsWritten = make(map[string]bool)
	for block := range receivedBlocks {
		receivedBlocksIsWritten[block] = false
	}
	// TODO: is there any way to parallelize this?
	batches := distributeBucketIDs(buckets, o.parameters.RedisPipelineSize)
	log.Debug().Msgf("Distributing %d buckets into %d batches for writing back", len(buckets), len(batches))
	bucketIDToBatchIndex := make(map[int]int)
	for i, bucketIDs := range batches {
		for _, bucketID := range bucketIDs {
			bucketIDToBatchIndex[bucketID] = i
		}
	}
	batchedBlocksFromReadBucket := make([]map[int]map[string]string, len(batches))
	for bucket, blockValues := range blocksFromReadBucket {
		if batchedBlocksFromReadBucket[bucketIDToBatchIndex[bucket]] == nil {
			batchedBlocksFromReadBucket[bucketIDToBatchIndex[bucket]] = make(map[int]map[string]string)
		}
		batchedBlocksFromReadBucket[bucketIDToBatchIndex[bucket]][bucket] = blockValues
	}

	for i, blocksFromReadBucket := range batchedBlocksFromReadBucket {
		log.Debug().Msgf("Batch %d: Writing %d buckets to storage %d", i, len(blocksFromReadBucket), storageID)
		writtenBlocks, err := o.storageHandler.BatchWriteBucket(storageID, blocksFromReadBucket, receivedBlocks)
		if err != nil {
			log.Error().Msgf("Failed to atomic write bucket for storageID %d: %s", storageID, err)
			return nil, fmt.Errorf("unable to atomic write bucket; %s", err)
		}
		log.Debug().Msgf("Written blocks from batch %d: %v", i, writtenBlocks)
		for block := range writtenBlocks {
			if _, exists := receivedBlocks[block]; exists {
				log.Debug().Msgf("Block %s was successfully written and is marked as such", block)
				delete(receivedBlocks, block)
				receivedBlocksIsWritten[block] = true
			}
		}
	}
	log.Debug().Msgf("Finished writing back blocks. Final written status: %v", receivedBlocksIsWritten)
	return receivedBlocksIsWritten, nil
}

func (o *oramNodeServer) evict(storageID int) error {
	log.Debug().Msgf("Starting eviction process for storageID %d", storageID)
	o.storageHandler.LockStorage(storageID)
	defer o.storageHandler.UnlockStorage(storageID)
	log.Debug().Msgf("Acquired lock for storageID %d", storageID)
	currentEvictionCount := o.oramNodeFSM.evictionCountMap[storageID]
	paths := o.storageHandler.GetMultipleReverseLexicographicPaths(currentEvictionCount, o.parameters.EvictPathCount)
	log.Debug().Msgf("Evicting with paths %v and storageID %d. Current eviction count: %d", paths, storageID, currentEvictionCount)
	beginEvictionCommand, err := newReplicateBeginEvictionCommand(currentEvictionCount, storageID)
	if err != nil {
		log.Error().Msgf("Failed to marshal begin eviction command: %s", err)
		return fmt.Errorf("unable to marshal begin eviction command; %s", err)
	}
	log.Debug().Msgf("Applying begin eviction command to Raft FSM")
	err = o.raftNode.Apply(beginEvictionCommand, 0).Error()
	if err != nil {
		log.Error().Msgf("Could not apply log to the FSM: %s", err)
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	log.Debug().Msgf("Successfully applied begin eviction command")

	buckets, err := o.storageHandler.GetBucketsInPaths(paths)
	if err != nil {
		log.Error().Msgf("Failed to get buckets for paths %v: %v", paths, err)
		return fmt.Errorf("unable to get buckets for paths; %v", err)
	}
	log.Debug().Msgf("Found %d buckets for paths %v", len(buckets), paths)
	blocksFromReadBucket, err := o.readAllBuckets(buckets, storageID)
	if err != nil {
		log.Error().Msgf("Failed to perform ReadBucket on all levels: %s", err)
		return fmt.Errorf("unable to perform ReadBucket on all levels")
	}
	log.Debug().Msgf("Finished reading all buckets. %d buckets read.", len(blocksFromReadBucket))

	randomShardNode := o.shardNodeRPCClients.getRandomShardNodeClient()
	log.Debug().Msgf("Selected random shard node client")
	receivedBlocks, err := o.readBlocksFromShardNode(paths, storageID, randomShardNode)
	log.Debug().Msgf("Received blocks from shardnode %v", receivedBlocks)
	if err != nil {
		log.Error().Msgf("Failed to read blocks from shard node: %s", err)
		return err
	}

	receivedBlocksIsWritten, err := o.writeBackBlocksToAllBuckets(buckets, storageID, blocksFromReadBucket, receivedBlocks)
	log.Debug().Msgf("Received blocks is written %v", receivedBlocksIsWritten)
	if err != nil {
		log.Error().Msgf("Failed to perform WriteBucket on all levels: %s", err)
		return fmt.Errorf("unable to perform WriteBucket on all levels; %s", err)
	}

	log.Debug().Msg("Sending acks/nacks to shard node")
	randomShardNode.sendBackAcksNacks(receivedBlocksIsWritten)

	endEvictionCommand, err := newReplicateEndEvictionCommand(currentEvictionCount+o.parameters.EvictPathCount, storageID)
	if err != nil {
		log.Error().Msgf("Failed to marshal end eviction command: %s", err)
		return fmt.Errorf("unable to marshal end eviction command; %s", err)
	}
	log.Debug().Msgf("Applying end eviction command to Raft FSM")
	err = o.raftNode.Apply(endEvictionCommand, 0).Error()
	if err != nil {
		log.Error().Msgf("Could not apply log to the FSM: %s", err)
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	log.Debug().Msgf("Successfully applied end eviction command")

	o.readPathCounter.Store(0)
	log.Debug().Msg("Reset read path counter to 0")

	return nil
}

func (o *oramNodeServer) getDistinctPathsInBatch(requests []*pb.BlockRequest) []int {
	log.Debug().Msg("Getting distinct paths from block requests")
	paths := make(map[int]bool)
	for _, request := range requests {
		paths[int(request.Path)] = true
	}
	var pathList []int
	for path := range paths {
		pathList = append(pathList, path)
	}
	log.Debug().Msgf("Found %d distinct paths: %v", len(pathList), pathList)
	return pathList
}

type blockOffsetResponse struct {
	offsets map[int]strg.BlockOffsetStatus
	err     error
}

func (o *oramNodeServer) asyncGetBlockOffset(bucketIDs []int, storageID int, blocks []string, responseChan chan blockOffsetResponse) {
	log.Debug().Msgf("Async getting block offsets for buckets %v and storageID %d", bucketIDs, storageID)
	offsets, err := o.storageHandler.BatchGetBlockOffset(bucketIDs, storageID, blocks)
	log.Debug().Msgf("Finished async get block offsets for buckets %v", bucketIDs)
	responseChan <- blockOffsetResponse{offsets: offsets, err: err}
}

type readBlockResponse struct {
	values map[int]string
	err    error
}

func (o *oramNodeServer) asyncReadBlock(offsets map[int]int, storageID int, responseChan chan readBlockResponse) {
	log.Debug().Msgf("Async reading blocks with offsets %v and storageID %d", offsets, storageID)
	values, err := o.storageHandler.BatchReadBlock(offsets, storageID)
	log.Debug().Msgf("Finished async read blocks for %d offsets", len(offsets))
	responseChan <- readBlockResponse{values: values, err: err}
}

func (o *oramNodeServer) ReadPath(ctx context.Context, request *pb.ReadPathRequest) (*pb.ReadPathReply, error) {
	if o.raftNode.State() != raft.Leader {
		log.Warn().Msgf("Received ReadPath request but this node is not the leader. State: %s", o.raftNode.State().String())
		return nil, fmt.Errorf(commonerrs.NotTheLeaderError)
	}
	log.Info().Msgf("Received read path request: %v", request)
	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, "oramnode read path request")
	defer span.End()
	o.storageHandler.LockStorage(int(request.StorageId))
	defer o.storageHandler.UnlockStorage(int(request.StorageId))
	log.Debug().Msgf("Acquired lock for storageID %d", request.StorageId)

	var blocks []string
	for _, req := range request.Requests {
		blocks = append(blocks, req.Block)
	}
	log.Debug().Msgf("Requests contain blocks: %v", blocks)

	realBlockBucketMapping := make(map[int]string) // map of bucket id to block

	paths := o.getDistinctPathsInBatch(request.Requests)
	beginReadPathCommand, err := newReplicateBeginReadPathCommand(paths, int(request.StorageId))
	if err != nil {
		log.Error().Msgf("Failed to create begin read path replication command: %v", err)
		return nil, fmt.Errorf("unable to create begin read path replication command; %v", err)
	}
	_, beginReadPathReplicationSpan := tracer.Start(ctx, "replicate begin read path")
	log.Debug().Msgf("Applying begin read path command to Raft FSM")
	err = o.raftNode.Apply(beginReadPathCommand, 0).Error()
	if err != nil {
		log.Error().Msgf("Could not apply log to the FSM: %s", err)
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	beginReadPathReplicationSpan.End()
	log.Debug().Msg("Successfully applied begin read path command")

	buckets, err := o.storageHandler.GetBucketsInPaths(paths)
	log.Debug().Msgf("Got buckets %v for paths %v", buckets, paths)
	if err != nil {
		log.Error().Msgf("Could not get bucket ids in the paths: %v", err)
		return nil, fmt.Errorf("could not get bucket ids in the paths; %v", err)
	}
	_, getBlockOffsetsSpan := tracer.Start(ctx, "get block offsets")
	offsetListResponseChan := make(chan blockOffsetResponse)
	batches := distributeBucketIDs(buckets, o.parameters.RedisPipelineSize)
	log.Debug().Msgf("Distributing %d buckets into %d batches for block offset retrieval", len(buckets), len(batches))
	for _, bucketIDs := range batches {
		go o.asyncGetBlockOffset(bucketIDs, int(request.StorageId), blocks, offsetListResponseChan)
	}
	getBlockOffsetsSpan.End()
	var offsetList []map[int]int // list of map of bucket id to offset
	for i := 0; i < len(batches); i++ {
		response := <-offsetListResponseChan
		if response.err != nil {
			log.Error().Msgf("Could not get offset from storage: %s", response.err)
			return nil, fmt.Errorf("could not get offset from storage")
		}
		offsetList = append(offsetList, make(map[int]int))
		for bucketID, offsetStatus := range response.offsets {
			if offsetStatus.IsReal {
				realBlockBucketMapping[bucketID] = offsetStatus.BlockFound
				log.Debug().Msgf("Found real block %s in bucket %d", offsetStatus.BlockFound, bucketID)
			}
			offsetList[i][bucketID] = offsetStatus.Offset
		}
	}
	log.Debug().Msgf("Got offsets for all batches: %v", offsetList)

	returnValues := make(map[string]string) // map of block to value
	for _, block := range blocks {
		returnValues[block] = ""
	}
	_, readBlocksSpan := tracer.Start(ctx, "read blocks")
	readBlockResponseChan := make(chan readBlockResponse)

	for _, offsets := range offsetList {
		go o.asyncReadBlock(offsets, int(request.StorageId), readBlockResponseChan)
	}
	for i := 0; i < len(offsetList); i++ {
		response := <-readBlockResponseChan
		if response.err != nil {
			log.Error().Msgf("Could not read block %v; %s", response.values, response.err)
			return nil, err
		}
		for bucketID, value := range response.values {
			if _, exists := realBlockBucketMapping[bucketID]; exists {
				returnValues[realBlockBucketMapping[bucketID]] = value
				log.Debug().Msgf("Retrieved value for real block %s from bucket %d", realBlockBucketMapping[bucketID], bucketID)
			}
		}
	}
	readBlocksSpan.End()
	log.Debug().Msgf("Going to return values %v", returnValues)

	_, earlyReshuffleSpan := tracer.Start(ctx, "early reshuffle")
	err = o.earlyReshuffle(buckets, int(request.StorageId))
	if err != nil {
		log.Error().Msgf("Early reshuffle failed: %s", err)
		return nil, fmt.Errorf("early reshuffle failed;%s", err)
	}
	earlyReshuffleSpan.End()

	o.readPathCounter.Add(1)
	log.Debug().Msgf("Read path counter incremented to %d", o.readPathCounter.Load())

	endReadPathCommand, err := newReplicateEndReadPathCommand()
	if err != nil {
		log.Error().Msgf("Failed to create end read path replication command: %s", err)
		return nil, fmt.Errorf("unable to create end read path replication command; %s", err)
	}
	_, endReadPathReplicationSpan := tracer.Start(ctx, "replicate end read path")
	log.Debug().Msgf("Applying end read path command to Raft FSM")
	err = o.raftNode.Apply(endReadPathCommand, 0).Error()
	if err != nil {
		log.Error().Msgf("Could not apply log to the FSM: %s", err)
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	endReadPathReplicationSpan.End()
	log.Debug().Msg("Successfully applied end read path command")

	var response []*pb.BlockResponse
	for block, value := range returnValues {
		response = append(response, &pb.BlockResponse{Block: block, Value: value})
	}
	log.Debug().Msgf("Returning read path response with %d blocks", len(response))
	return &pb.ReadPathReply{Responses: response}, nil
}

func (o *oramNodeServer) JoinRaftVoter(ctx context.Context, joinRaftVoterRequest *pb.JoinRaftVoterRequest) (*pb.JoinRaftVoterReply, error) {
	requestingNodeId := joinRaftVoterRequest.NodeId
	requestingNodeAddr := joinRaftVoterRequest.NodeAddr

	log.Printf("received join request from node %d at %s", requestingNodeId, requestingNodeAddr)
	log.Debug().Msgf("Attempting to add voter: NodeID=%d, Address=%s", requestingNodeId, requestingNodeAddr)

	err := o.raftNode.AddVoter(
		raft.ServerID(strconv.Itoa(int(requestingNodeId))),
		raft.ServerAddress(requestingNodeAddr),
		0, 0).Error()

	if err != nil {
		log.Error().Msgf("Voter could not be added to the leader: %s", err)
		return &pb.JoinRaftVoterReply{Success: false}, fmt.Errorf("voter could not be added to the leader; %s", err)
	}
	log.Info().Msgf("Successfully added voter node %d", requestingNodeId)
	return &pb.JoinRaftVoterReply{Success: true}, nil
}

func StartServer(oramNodeServerID int, bindIP string, advIP string, rpcPort int, replicaID int, raftPort int, joinAddr string, shardNodeRPCClients map[int]ReplicaRPCClientMap, redisEndpoints []config.RedisEndpoint, parameters config.Parameters) {
	log.Info().Msg("Starting ORAM Node Server")
	isFirst := joinAddr == ""
	log.Debug().Msgf("Is this the first node in the cluster? %v", isFirst)
	oramNodeFSM := newOramNodeFSM()
	r, err := startRaftServer(isFirst, bindIP, advIP, replicaID, raftPort, oramNodeFSM)
	if err != nil {
		log.Fatal().Msgf("The raft node creation did not succeed: %s", err)
	}

	if !isFirst {
		log.Info().Msgf("This is not the first node. Attempting to join the cluster at %s", joinAddr)
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal().Msgf("The raft node could not connect to the leader as a new voter: %s", err)
		}
		client := pb.NewOramNodeClient(conn)
		log.Debug().Msgf("Sending join request to leader at %s", joinAddr)
		joinRaftVoterReply, err := client.JoinRaftVoter(
			context.Background(),
			&pb.JoinRaftVoterRequest{
				NodeId:   int32(replicaID),
				NodeAddr: fmt.Sprintf("%s:%d", advIP, raftPort),
			},
		)
		if err != nil || !joinRaftVoterReply.Success {
			log.Error().Msgf("The raft node could not connect to the leader as a new voter: %s", err)
		} else {
			log.Info().Msg("Successfully joined the raft cluster.")
		}
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindIP, rpcPort))
	if err != nil {
		log.Fatal().Msgf("Failed to listen on %s:%d: %v", bindIP, rpcPort, err)
	}
	var storages []config.RedisEndpoint
	for _, redisEndpoint := range redisEndpoints {
		if redisEndpoint.ORAMNodeID == oramNodeServerID {
			storages = append(storages, redisEndpoint)
		}
	}
	log.Debug().Msgf("Found %d storage endpoints for ORAM Node ID %d", len(storages), oramNodeServerID)
	storageHandler := strg.NewStorageHandler(parameters.TreeHeight, parameters.Z, parameters.S, parameters.Shift, storages)
	if isFirst {
		log.Info().Msg("First node initializing the database")
		err = storageHandler.InitDatabase()
		if err != nil {
			log.Fatal().Msgf("Failed to initialize the database: %v", err)
		}
	}
	oramNodeServer := newOramNodeServer(oramNodeServerID, replicaID, r, oramNodeFSM, shardNodeRPCClients, storageHandler, parameters)
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			currentReadPathCount := oramNodeServer.readPathCounter.Load()
			log.Debug().Msgf("Current read path counter: %d, Eviction rate: %d", currentReadPathCount, oramNodeServer.parameters.EvictionRate)
			if currentReadPathCount >= int32(oramNodeServer.parameters.EvictionRate) {
				log.Info().Msgf("Read path counter reached eviction threshold. Starting eviction.")
				storageID := oramNodeServer.storageHandler.GetRandomStorageID()
				log.Debug().Msgf("Selected random storage ID %d for eviction", storageID)
				oramNodeServer.evict(storageID)
			}
		}
	}()
	go func() {
		for {
			oramNodeServer.performFailedOperations()
		}
	}()
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	pb.RegisterOramNodeServer(grpcServer, oramNodeServer)
	log.Info().Msgf("ORAM Node Server listening on %s:%d", bindIP, rpcPort)
	grpcServer.Serve(lis)
}
