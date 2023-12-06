package oramnode

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	pb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/commonerrs"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	strg "github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type storage interface {
	GetMaxAccessCount() int
	LockStorage(storageID int)
	UnlockStorage(storageID int)
	GetBlockOffset(bucketID int, storageID int, blocks []string) (offset int, isReal bool, blockFound string, err error)
	GetAccessCount(bucketID int, storageID int) (count int, err error)
	ReadBucket(bucketID int, storageID int) (blocks map[string]string, err error)
	WriteBucket(bucketID int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error)
	ReadBlock(bucketID int, storageID int, offset int) (value string, err error)
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
	shardNodeRPCClients map[int]ReplicaRPCClientMap
	readPathCounter     atomic.Int32
	storageHandler      storage
	parameters          config.Parameters
}

func newOramNodeServer(oramNodeServerID int, replicaID int, raftNode *raft.Raft, oramNodeFSM *oramNodeFSM, shardNodeRPCClients map[int]ReplicaRPCClientMap, storageHandler storage, parameters config.Parameters) *oramNodeServer {
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
	<-o.raftNode.LeaderCh()
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

func (o *oramNodeServer) earlyReshuffle(buckets []int, storageID int) error {
	log.Debug().Msgf("Performing early reshuffle with buckets %v and storageID %d", buckets, storageID)
	// TODO: can we make this a background thread?
	errorChan := make(chan error)
	for _, bucket := range buckets {
		go func(bucket int) {
			accessCount, err := o.storageHandler.GetAccessCount(bucket, storageID)
			if err != nil {
				errorChan <- fmt.Errorf("unable to get access count from the server; %s", err)
				return
			}
			if accessCount < o.storageHandler.GetMaxAccessCount() {
				errorChan <- nil
				return
			}
			localStash, err := o.storageHandler.ReadBucket(bucket, storageID)
			if err != nil {
				errorChan <- fmt.Errorf("unable to read bucket from the server; %s", err)
				return
			}
			writtenBlocks, err := o.storageHandler.WriteBucket(bucket, storageID, localStash, nil)
			if err != nil {
				errorChan <- fmt.Errorf("unable to write bucket from the server; %s", err)
				return
			}
			for block := range localStash {
				if _, exists := writtenBlocks[block]; !exists {
					errorChan <- fmt.Errorf("unable to write all blocks to the bucket")
					return
				}
			}
			errorChan <- nil
		}(bucket)
	}
	for i := 0; i < len(buckets); i++ {
		err := <-errorChan
		if err != nil {
			return err
		}
	}
	return nil
}

type readBucketResponse struct {
	bucket int
	blocks map[string]string
	err    error
}

func (o *oramNodeServer) asyncReadBucket(bucket int, storageID int, responseChan chan readBucketResponse) {
	blocks, err := o.storageHandler.ReadBucket(bucket, storageID)
	responseChan <- readBucketResponse{bucket: bucket, blocks: blocks, err: err}
}

func (o *oramNodeServer) readAllBuckets(buckets []int, storageID int) (blocksFromReadBucket map[int]map[string]string, err error) {
	log.Debug().Msgf("Reading all buckets with buckets %v and storageID %d", buckets, storageID)
	blocksFromReadBucket = make(map[int]map[string]string) // map of bucket to map of block to value
	if err != nil {
		return nil, fmt.Errorf("unable to get bucket ids for early reshuffle path; %v", err)
	}
	readBucketResponseChan := make(chan readBucketResponse)
	for _, bucket := range buckets {
		go o.asyncReadBucket(bucket, storageID, readBucketResponseChan)
	}
	for i := 0; i < len(buckets); i++ {
		response := <-readBucketResponseChan
		log.Debug().Msgf("Got blocks %v from bucket %d", response.blocks, response.bucket)
		if err != nil {
			return nil, fmt.Errorf("unable to read bucket; %s", err)
		}
		if blocksFromReadBucket[response.bucket] == nil {
			blocksFromReadBucket[response.bucket] = make(map[string]string)
		}
		for block, value := range response.blocks {
			blocksFromReadBucket[response.bucket][block] = value
		}
	}
	return blocksFromReadBucket, nil
}

func (o *oramNodeServer) readBlocksFromShardNode(paths []int, storageID int, randomShardNode ReplicaRPCClientMap) (receivedBlocks map[string]string, err error) {
	log.Debug().Msgf("Reading blocks from shard node with paths %v and storageID %d", paths, storageID)
	receivedBlocks = make(map[string]string) // map of received block to value

	shardNodeBlocks, err := randomShardNode.getBlocksFromShardNode(paths, storageID, o.parameters.MaxBlocksToSend)
	if err != nil {
		return nil, fmt.Errorf("unable to get blocks from shard node; %s", err)
	}
	for _, block := range shardNodeBlocks {
		receivedBlocks[block.Block] = block.Value
	}
	return receivedBlocks, nil
}

func (o *oramNodeServer) writeBackBlocksToAllBuckets(buckets []int, storageID int, blocksFromReadBucket map[int]map[string]string, receivedBlocks map[string]string) (receivedBlocksIsWritten map[string]bool, err error) {
	log.Debug().Msgf("Writing back blocks to all buckets with buckets %v and storageID %d", buckets, storageID)
	receivedBlocksCopy := make(map[string]string)
	for block, value := range receivedBlocks {
		receivedBlocksCopy[block] = value
	}
	receivedBlocksIsWritten = make(map[string]bool)
	for block := range receivedBlocks {
		receivedBlocksIsWritten[block] = false
	}
	for i := len(buckets) - 1; i >= 0; i-- {
		writtenBlocks, err := o.storageHandler.WriteBucket(buckets[i], storageID, blocksFromReadBucket[buckets[i]], receivedBlocksCopy)
		if err != nil {
			return nil, fmt.Errorf("unable to atomic write bucket; %s", err)
		}
		for block := range writtenBlocks {
			if _, exists := receivedBlocksCopy[block]; exists {
				delete(receivedBlocksCopy, block)
				receivedBlocksIsWritten[block] = true
			}
		}
	}
	return receivedBlocksIsWritten, nil
}

func (o *oramNodeServer) evict(storageID int) error {
	o.storageHandler.LockStorage(storageID)
	defer o.storageHandler.UnlockStorage(storageID)
	currentEvictionCount := o.oramNodeFSM.evictionCountMap[storageID]
	paths := o.storageHandler.GetMultipleReverseLexicographicPaths(currentEvictionCount, o.parameters.EvictPathCount)
	log.Debug().Msgf("Evicting with paths %v and storageID %d", paths, storageID)
	beginEvictionCommand, err := newReplicateBeginEvictionCommand(currentEvictionCount, storageID)
	if err != nil {
		return fmt.Errorf("unable to marshal begin eviction command; %s", err)
	}
	err = o.raftNode.Apply(beginEvictionCommand, 0).Error()
	if err != nil {
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	buckets, err := o.storageHandler.GetBucketsInPaths(paths)
	if err != nil {
		return fmt.Errorf("unable to get buckets for paths; %v", err)
	}
	blocksFromReadBucket, err := o.readAllBuckets(buckets, storageID)
	if err != nil {
		return fmt.Errorf("unable to perform ReadBucket on all levels")
	}

	// TODO: get blocks from multiple random shard nodes
	randomShardNode := o.shardNodeRPCClients[0]
	receivedBlocks, err := o.readBlocksFromShardNode(paths, storageID, randomShardNode)
	if err != nil {
		return err
	}

	receivedBlocksIsWritten, err := o.writeBackBlocksToAllBuckets(buckets, storageID, blocksFromReadBucket, receivedBlocks)
	log.Debug().Msgf("Received blocks is written %v", receivedBlocksIsWritten)
	if err != nil {
		return fmt.Errorf("unable to perform WriteBucket on all levels; %s", err)
	}

	randomShardNode.sendBackAcksNacks(receivedBlocksIsWritten)

	endEvictionCommand, err := newReplicateEndEvictionCommand(currentEvictionCount+o.parameters.EvictPathCount, storageID)
	if err != nil {
		return fmt.Errorf("unable to marshal end eviction command; %s", err)
	}
	err = o.raftNode.Apply(endEvictionCommand, 0).Error()
	if err != nil {
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	o.readPathCounter.Store(0)

	return nil
}

func (o *oramNodeServer) getDistinctPathsInBatch(requests []*pb.BlockRequest) []int {
	paths := make(map[int]bool)
	for _, request := range requests {
		paths[int(request.Path)] = true
	}
	var pathList []int
	for path := range paths {
		pathList = append(pathList, path)
	}
	return pathList
}

type blockOffsetResponse struct {
	bucketID   int
	offset     int
	isReal     bool
	blockFound string
	err        error
}

func (o *oramNodeServer) asyncGetBlockOffset(bucketID int, storageID int, blocks []string, responseChan chan blockOffsetResponse) {
	offset, isReal, blockFound, err := o.storageHandler.GetBlockOffset(bucketID, storageID, blocks)
	responseChan <- blockOffsetResponse{bucketID: bucketID, offset: offset, isReal: isReal, blockFound: blockFound, err: err}
}

type readBlockResponse struct {
	block string
	value string
	err   error
}

func (o *oramNodeServer) asyncReadBlock(block string, bucketID int, storageID int, offset int, responseChan chan readBlockResponse) {
	value, err := o.storageHandler.ReadBlock(bucketID, storageID, offset)
	responseChan <- readBlockResponse{block: block, value: value, err: err}
}

func (o *oramNodeServer) ReadPath(ctx context.Context, request *pb.ReadPathRequest) (*pb.ReadPathReply, error) {
	if o.raftNode.State() != raft.Leader {
		return nil, fmt.Errorf(commonerrs.NotTheLeaderError)
	}
	log.Debug().Msgf("Received read path request %v", request)
	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, "oramnode read path request")
	o.storageHandler.LockStorage(int(request.StorageId))
	defer o.storageHandler.UnlockStorage(int(request.StorageId))

	var blocks []string
	for _, request := range request.Requests {
		blocks = append(blocks, request.Block)
	}

	offsetList := make(map[int]int)                // map of bucket id to offset
	realBlockBucketMapping := make(map[int]string) // map of bucket id to block

	paths := o.getDistinctPathsInBatch(request.Requests)
	beginReadPathCommand, err := newReplicateBeginReadPathCommand(paths, int(request.StorageId))
	if err != nil {
		return nil, fmt.Errorf("unable to create begin read path replication command; %v", err)
	}
	_, beginReadPathReplicationSpan := tracer.Start(ctx, "replicate begin read path")
	err = o.raftNode.Apply(beginReadPathCommand, 0).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	beginReadPathReplicationSpan.End()

	buckets, err := o.storageHandler.GetBucketsInPaths(paths)
	log.Debug().Msgf("Got buckets %v", buckets)
	if err != nil {
		return nil, fmt.Errorf("could not get bucket ids in the paths; %v", err)
	}
	_, getBlockOffsetsSpan := tracer.Start(ctx, "get block offsets")
	offsetListResponseChan := make(chan blockOffsetResponse)
	for _, bucketID := range buckets {
		go o.asyncGetBlockOffset(bucketID, int(request.StorageId), blocks, offsetListResponseChan)
	}
	getBlockOffsetsSpan.End()
	for i := 0; i < len(buckets); i++ {
		response := <-offsetListResponseChan
		if response.err != nil {
			return nil, fmt.Errorf("could not get offset from storage")
		}
		if response.isReal {
			realBlockBucketMapping[response.bucketID] = response.blockFound
		}
		offsetList[response.bucketID] = response.offset
	}
	log.Debug().Msgf("Got offsets %v", offsetList)

	returnValues := make(map[string]string) // map of block to value
	for _, block := range blocks {
		returnValues[block] = ""
	}
	_, readBlocksSpan := tracer.Start(ctx, "read blocks")
	readBlockResponseChan := make(chan readBlockResponse)
	realReadCount := 0
	for _, bucketID := range buckets {
		if block, exists := realBlockBucketMapping[bucketID]; exists {
			go o.asyncReadBlock(block, bucketID, int(request.StorageId), offsetList[bucketID], readBlockResponseChan)
			realReadCount++
		} else {
			go o.storageHandler.ReadBlock(bucketID, int(request.StorageId), offsetList[bucketID])
		}
	}
	for i := 0; i < realReadCount; i++ {
		response := <-readBlockResponseChan
		if response.err != nil {
			log.Error().Msgf("Could not read block %s; %s", response.block, response.err)
			return nil, err
		}
		returnValues[response.block] = response.value
	}
	readBlocksSpan.End()
	log.Debug().Msgf("Going to return values %v", returnValues)

	_, earlyReshuffleSpan := tracer.Start(ctx, "early reshuffle")
	err = o.earlyReshuffle(buckets, int(request.StorageId))
	if err != nil {
		return nil, fmt.Errorf("early reshuffle failed;%s", err)
	}
	earlyReshuffleSpan.End()

	o.readPathCounter.Add(1)

	endReadPathCommand, err := newReplicateEndReadPathCommand()
	if err != nil {
		return nil, fmt.Errorf("unable to create end read path replication command; %s", err)
	}
	_, endReadPathReplicationSpan := tracer.Start(ctx, "replicate end read path")
	err = o.raftNode.Apply(endReadPathCommand, 0).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	endReadPathReplicationSpan.End()

	var response []*pb.BlockResponse
	for block, value := range returnValues {
		response = append(response, &pb.BlockResponse{Block: block, Value: value})
	}
	span.End()
	log.Debug().Msgf("Returning read path response %v", response)
	return &pb.ReadPathReply{Responses: response}, nil
}

func (o *oramNodeServer) JoinRaftVoter(ctx context.Context, joinRaftVoterRequest *pb.JoinRaftVoterRequest) (*pb.JoinRaftVoterReply, error) {
	requestingNodeId := joinRaftVoterRequest.NodeId
	requestingNodeAddr := joinRaftVoterRequest.NodeAddr

	log.Printf("received join request from node %d at %s", requestingNodeId, requestingNodeAddr)

	err := o.raftNode.AddVoter(
		raft.ServerID(strconv.Itoa(int(requestingNodeId))),
		raft.ServerAddress(requestingNodeAddr),
		0, 0).Error()

	if err != nil {
		return &pb.JoinRaftVoterReply{Success: false}, fmt.Errorf("voter could not be added to the leader; %s", err)
	}
	return &pb.JoinRaftVoterReply{Success: true}, nil
}

func StartServer(oramNodeServerID int, ip string, rpcPort int, replicaID int, raftPort int, joinAddr string, shardNodeRPCClients map[int]ReplicaRPCClientMap, redisEndpoints []config.RedisEndpoint, parameters config.Parameters) {
	isFirst := joinAddr == ""
	oramNodeFSM := newOramNodeFSM()
	r, err := startRaftServer(isFirst, ip, replicaID, raftPort, oramNodeFSM)
	if err != nil {
		log.Fatal().Msgf("The raft node creation did not succeed; %s", err)
	}

	if !isFirst {
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal().Msgf("The raft node could not connect to the leader as a new voter; %s", err)
		}
		client := pb.NewOramNodeClient(conn)
		joinRaftVoterReply, err := client.JoinRaftVoter(
			context.Background(),
			&pb.JoinRaftVoterRequest{
				NodeId:   int32(replicaID),
				NodeAddr: fmt.Sprintf("%s:%d", ip, raftPort),
			},
		)
		if err != nil || !joinRaftVoterReply.Success {
			log.Error().Msgf("The raft node could not connect to the leader as a new voter; %s", err)
		}
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, rpcPort))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}
	var storages []config.RedisEndpoint
	for _, redisEndpoint := range redisEndpoints {
		if redisEndpoint.ORAMNodeID == oramNodeServerID {
			storages = append(storages, redisEndpoint)
		}
	}
	storageHandler := strg.NewStorageHandler(parameters.TreeHeight, parameters.Z, parameters.S, parameters.Shift, storages)
	if isFirst {
		err = storageHandler.InitDatabase()
		if err != nil {
			log.Fatal().Msgf("failed to initialize the database: %v", err)
		}
	}
	oramNodeServer := newOramNodeServer(oramNodeServerID, replicaID, r, oramNodeFSM, shardNodeRPCClients, storageHandler, parameters)
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			if oramNodeServer.readPathCounter.Load() >= int32(oramNodeServer.parameters.EvictionRate) {
				storageID := oramNodeServer.storageHandler.GetRandomStorageID()
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
	grpcServer.Serve(lis)
}
