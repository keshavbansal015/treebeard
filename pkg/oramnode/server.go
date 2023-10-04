package oramnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	pb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	strg "github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/hashicorp/raft"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type storage interface {
	GetMaxAccessCount() int
	GetRandomPathAndStorageID(context.Context) (path int, storageID int)
	GetBlockOffset(bucketID int, storageID int, blocks []string) (offset int, isReal bool, blockFound string, err error)
	GetAccessCount(bucketID int, storageID int) (count int, err error)
	ReadBucket(bucketID int, storageID int) (blocks map[string]string, err error)
	WriteBucket(bucketID int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error)
	ReadBlock(bucketID int, storageID int, offset int) (value string, err error)
	GetBucketsInPaths(paths []int) (bucketIDs []int, err error)
}

type oramNodeServer struct {
	pb.UnimplementedOramNodeServer
	oramNodeServerID    int
	replicaID           int
	raftNode            *raft.Raft
	oramNodeFSM         *oramNodeFSM
	shardNodeRPCClients map[int]ReplicaRPCClientMap
	readPathCounter     int
	readPathCounterMu   sync.Mutex
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
		readPathCounter:     0,
		storageHandler:      storageHandler,
		parameters:          parameters,
	}
}

// It runs the failed eviction and read path as the new leader.
func (o *oramNodeServer) performFailedOperations() error {
	<-o.raftNode.LeaderCh()
	o.oramNodeFSM.mu.Lock()
	needsEviction := o.oramNodeFSM.unfinishedEviction
	needsReadPath := o.oramNodeFSM.unfinishedReadPath
	o.oramNodeFSM.mu.Unlock()
	if needsEviction != nil {
		o.oramNodeFSM.mu.Lock()
		paths := o.oramNodeFSM.unfinishedEviction.paths
		storageID := o.oramNodeFSM.unfinishedEviction.storageID
		o.oramNodeFSM.mu.Unlock()
		o.evict(paths, storageID)
	}
	if needsReadPath != nil {
		o.oramNodeFSM.mu.Lock()
		paths := o.oramNodeFSM.unfinishedReadPath.paths
		storageID := o.oramNodeFSM.unfinishedReadPath.storageID
		o.oramNodeFSM.mu.Unlock()
		buckets, _ := o.storageHandler.GetBucketsInPaths(paths)
		o.earlyReshuffle(buckets, storageID)
	}
	return nil
}

func (o *oramNodeServer) earlyReshuffle(buckets []int, storageID int) error {
	// TODO: can we make this a background thread?
	for _, bucket := range buckets {
		// TODO: check the Redis latency
		accessCount, err := o.storageHandler.GetAccessCount(bucket, storageID)
		if err != nil {
			return fmt.Errorf("unable to get access count from the server; %s", err)
		}
		if accessCount < o.storageHandler.GetMaxAccessCount() {
			continue
		}
		localStash, err := o.storageHandler.ReadBucket(bucket, storageID)
		if err != nil {
			return fmt.Errorf("unable to read bucket from the server; %s", err)
		}
		writtenBlocks, err := o.storageHandler.WriteBucket(bucket, storageID, localStash, nil, false)
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

func (o *oramNodeServer) readAllBuckets(buckets []int, storageID int) (blocksFromReadBucket map[int]map[string]string, err error) {
	blocksFromReadBucket = make(map[int]map[string]string) // map of bucket to map of block to value
	if err != nil {
		return nil, fmt.Errorf("unable to get bucket ids for early reshuffle path; %v", err)
	}
	for _, bucket := range buckets {
		blocks, err := o.storageHandler.ReadBucket(bucket, storageID)
		if err != nil {
			return nil, fmt.Errorf("unable to read bucket; %s", err)
		}
		if blocksFromReadBucket[bucket] == nil {
			blocksFromReadBucket[bucket] = make(map[string]string)
		}
		for block, value := range blocks {
			blocksFromReadBucket[bucket][block] = value
		}
	}
	return blocksFromReadBucket, nil
}

func (o *oramNodeServer) readBlocksFromShardNode(paths []int, storageID int, randomShardNode ReplicaRPCClientMap) (receivedBlocks map[string]string, err error) {
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
	receivedBlocksCopy := make(map[string]string)
	for block, value := range receivedBlocks {
		receivedBlocksCopy[block] = value
	}
	receivedBlocksIsWritten = make(map[string]bool)
	for block := range receivedBlocks {
		receivedBlocksIsWritten[block] = false
	}
	for i := len(buckets) - 1; i >= 0; i-- {
		writtenBlocks, err := o.storageHandler.WriteBucket(buckets[i], storageID, blocksFromReadBucket[buckets[i]], receivedBlocksCopy, true)
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

func (o *oramNodeServer) evict(paths []int, storageID int) error {
	beginEvictionCommand, err := newReplicateBeginEvictionCommand(paths, storageID)
	if err != nil {
		return fmt.Errorf("unable to marshal begin eviction command; %s", err)
	}
	err = o.raftNode.Apply(beginEvictionCommand, 2*time.Second).Error()
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
	if err != nil {
		return fmt.Errorf("unable to perform WriteBucket on all levels; %s", err)
	}

	randomShardNode.sendBackAcksNacks(receivedBlocksIsWritten)

	endEvictionCommand, err := newReplicateEndEvictionCommand()
	if err != nil {
		return fmt.Errorf("unable to marshal end eviction command; %s", err)
	}
	err = o.raftNode.Apply(endEvictionCommand, 2*time.Second).Error()
	if err != nil {
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	o.readPathCounterMu.Lock()
	o.readPathCounter = 0
	o.readPathCounterMu.Unlock()

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

func (o *oramNodeServer) ReadPath(ctx context.Context, request *pb.ReadPathRequest) (*pb.ReadPathReply, error) {
	if o.raftNode.State() != raft.Leader {
		return nil, fmt.Errorf("not the leader node")
	}
	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, "oramnode read path request")

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
	err = o.raftNode.Apply(beginReadPathCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	buckets, err := o.storageHandler.GetBucketsInPaths(paths)
	if err != nil {
		return nil, fmt.Errorf("could not get bucket ids in the paths; %v", err)
	}
	for _, bucketID := range buckets {
		offset, isReal, blockFound, err := o.storageHandler.GetBlockOffset(bucketID, int(request.StorageId), blocks)
		if err != nil {
			return nil, fmt.Errorf("could not get offset from storage")
		}
		if isReal {
			realBlockBucketMapping[bucketID] = blockFound
		}
		offsetList[bucketID] = offset
	}

	returnValues := make(map[string]string) // map of block to value
	for _, block := range blocks {
		returnValues[block] = ""
	}
	for _, bucketID := range buckets {
		if block, exists := realBlockBucketMapping[bucketID]; exists {
			value, err := o.storageHandler.ReadBlock(bucketID, int(request.StorageId), offsetList[bucketID])
			if err != nil {
				return nil, err
			}
			returnValues[block] = value
		} else {
			o.storageHandler.ReadBlock(bucketID, int(request.StorageId), offsetList[bucketID])
		}
	}

	err = o.earlyReshuffle(buckets, int(request.StorageId))
	if err != nil {
		return nil, fmt.Errorf("early reshuffle failed;%s", err)
	}

	o.readPathCounterMu.Lock()
	o.readPathCounter++
	o.readPathCounterMu.Unlock()

	endReadPathCommand, err := newReplicateEndReadPathCommand()
	if err != nil {
		return nil, fmt.Errorf("unable to create end read path replication command; %s", err)
	}
	err = o.raftNode.Apply(endReadPathCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	var response []*pb.BlockResponse
	for block, value := range returnValues {
		response = append(response, &pb.BlockResponse{Block: block, Value: value})
	}
	span.End()
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

func StartServer(oramNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string, shardNodeRPCClients map[int]ReplicaRPCClientMap, parameters config.Parameters) {
	isFirst := joinAddr == ""
	oramNodeFSM := newOramNodeFSM()
	r, err := startRaftServer(isFirst, replicaID, raftPort, oramNodeFSM)
	if err != nil {
		log.Fatalf("The raft node creation did not succeed; %s", err)
	}

	go func() {
		for {
			time.Sleep(5 * time.Second)
			fmt.Println(oramNodeFSM)
		}
	}()

	if !isFirst {
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("The raft node could not connect to the leader as a new voter; %s", err)
		}
		client := pb.NewOramNodeClient(conn)
		fmt.Println(raftPort)
		joinRaftVoterReply, err := client.JoinRaftVoter(
			context.Background(),
			&pb.JoinRaftVoterRequest{
				NodeId:   int32(replicaID),
				NodeAddr: fmt.Sprintf("localhost:%d", raftPort),
			},
		)
		if err != nil || !joinRaftVoterReply.Success {
			log.Fatalf("The raft node could not connect to the leader as a new voter; %s", err)
		}
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", rpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	oramNodeServer := newOramNodeServer(oramNodeServerID, replicaID, r, oramNodeFSM, shardNodeRPCClients, strg.NewStorageHandler(), parameters)
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			needEviction := false
			oramNodeServer.readPathCounterMu.Lock()
			if oramNodeServer.readPathCounter >= oramNodeServer.parameters.EvictionRate {
				needEviction = true
			}
			oramNodeServer.readPathCounterMu.Unlock()
			if needEviction {
				oramNodeServer.evict([]int{0, 1, 2}, 0) // TODO: make it lexicographic
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
