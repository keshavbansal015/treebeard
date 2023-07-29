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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type storage interface {
	GetLevelCount() int
	GetMaxAccessCount() int
	GetRandomPathAndStorageID() (path int, storageID int)
	GetBlockOffset(level int, path int, storageID int, block string) (offset int, err error)
	GetAccessCount(level int, path int, storageID int) (count int, err error)
	ReadBucket(level int, path int, storageID int) (blocks map[string]string, err error)
	WriteBucket(level int, path int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error)
	ReadBlock(level int, path int, storageID int, offset int) (value string, err error)
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

func newOramNodeServer(oramNodeServerID int, replicaID int, raftNode *raft.Raft, oramNodeFSM *oramNodeFSM, shardNodeRPCClients map[int]ReplicaRPCClientMap, storageHandler *strg.StorageHandler, parameters config.Parameters) *oramNodeServer {
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

func (o *oramNodeServer) performFailedEviction() error {
	<-o.raftNode.LeaderCh()
	o.oramNodeFSM.mu.Lock()
	needsEviction := o.oramNodeFSM.unfinishedEviction
	o.oramNodeFSM.mu.Unlock()
	if needsEviction != nil {
		o.oramNodeFSM.mu.Lock()
		path := o.oramNodeFSM.unfinishedEviction.path
		storageID := o.oramNodeFSM.unfinishedEviction.storageID
		o.oramNodeFSM.mu.Unlock()
		o.evict(path, storageID)
	}
	return nil
}

func (o *oramNodeServer) earlyReshuffle(path int, storageID int) error {
	for level := 0; level < o.storageHandler.GetLevelCount(); level++ {
		accessCount, err := o.storageHandler.GetAccessCount(level, path, storageID)
		if err != nil {
			return fmt.Errorf("unable to get access count from the server; %s", err)
		}
		if accessCount < o.storageHandler.GetMaxAccessCount() {
			continue
		}
		localStash, err := o.storageHandler.ReadBucket(level, path, storageID)
		if err != nil {
			return fmt.Errorf("unable to read bucket from the server; %s", err)
		}
		writtenBlocks, err := o.storageHandler.WriteBucket(level, path, storageID, localStash, nil, false)
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

func (o *oramNodeServer) readBucketAllLevels(path int, storageID int) (blocksFromReadBucket map[int]map[string]string, err error) {
	blocksFromReadBucket = make(map[int]map[string]string) // map of bucket to map of block to value
	for level := 0; level < o.storageHandler.GetLevelCount(); level++ {
		blocks, err := o.storageHandler.ReadBucket(level, path, storageID)
		if err != nil {
			return nil, fmt.Errorf("unable to read bucket; %s", err)
		}
		if blocksFromReadBucket[level] == nil {
			blocksFromReadBucket[level] = make(map[string]string)
		}
		for block, value := range blocks {
			blocksFromReadBucket[level][block] = value
		}
	}
	return blocksFromReadBucket, nil
}

func (o *oramNodeServer) readBlocksFromShardNode(path int, storageID int, randomShardNode ReplicaRPCClientMap) (receivedBlocks map[string]string, err error) {
	receivedBlocks = make(map[string]string) // map of received block to value

	shardNodeBlocks, err := randomShardNode.getBlocksFromShardNode(path, storageID, o.parameters.MaxBlocksToSend)
	if err != nil {
		return nil, fmt.Errorf("unable to get blocks from shard node; %s", err)
	}
	for _, block := range shardNodeBlocks {
		receivedBlocks[block.Block] = block.Value
	}
	return receivedBlocks, nil
}

func (o *oramNodeServer) writeBackBlocksToAllLevels(path int, storageID int, blocksFromReadBucket map[int]map[string]string, receivedBlocks map[string]string) (receivedBlocksIsWritten map[string]bool, err error) {
	receivedBlocksCopy := make(map[string]string)
	for block, value := range receivedBlocks {
		receivedBlocksCopy[block] = value
	}
	receivedBlocksIsWritten = make(map[string]bool)
	for block := range receivedBlocks {
		receivedBlocksIsWritten[block] = false
	}
	for level := o.storageHandler.GetLevelCount() - 1; level >= 0; level-- {
		writtenBlocks, err := o.storageHandler.WriteBucket(level, path, storageID, blocksFromReadBucket[level], receivedBlocksCopy, true)
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

func (o *oramNodeServer) evict(path int, storageID int) error {
	beginEvictionCommand, err := newReplicateBeginEvictionCommand(path, storageID)
	if err != nil {
		return fmt.Errorf("unable to marshal begin eviction command; %s", err)
	}
	err = o.raftNode.Apply(beginEvictionCommand, 2*time.Second).Error()
	if err != nil {
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	blocksFromReadBucket, err := o.readBucketAllLevels(path, storageID)
	if err != nil {
		return fmt.Errorf("unable to perform ReadBucket on all levels")
	}

	// TODO: get blocks from multiple random shard nodes
	randomShardNode := o.shardNodeRPCClients[0]
	receivedBlocks, err := o.readBlocksFromShardNode(path, storageID, randomShardNode)
	if err != nil {
		return err
	}

	receivedBlocksIsWritten, err := o.writeBackBlocksToAllLevels(path, storageID, blocksFromReadBucket, receivedBlocks)
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

func (o *oramNodeServer) ReadPath(ctx context.Context, request *pb.ReadPathRequest) (*pb.ReadPathReply, error) {
	if o.raftNode.State() != raft.Leader {
		return nil, fmt.Errorf("not the leader node")
	}

	var offsetList []int
	o.oramNodeFSM.mu.Lock()
	if _, exists := o.oramNodeFSM.offsetListMap[request.Block]; exists {
		offsetList = o.oramNodeFSM.offsetListMap[request.Block]
	}
	o.oramNodeFSM.mu.Unlock()

	if offsetList == nil {
		for level := 0; level < o.storageHandler.GetLevelCount(); level++ {
			offset, err := o.storageHandler.GetBlockOffset(level, int(request.Path), int(request.StorageId), request.Block)
			if err != nil {
				return nil, fmt.Errorf("could not get offset from storage")
			}
			offsetList = append(offsetList, offset)
		}
		replicateOffsetAndBeginReadPathCommand, err := newReplicateOffsetListCommand(request.Block, offsetList)
		if err != nil {
			return nil, fmt.Errorf("could not create offsetList replication command; %s", err)
		}
		err = o.raftNode.Apply(replicateOffsetAndBeginReadPathCommand, 2*time.Second).Error()
		if err != nil {
			return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
		}
	}

	var returnValue string
	for level := 0; level < o.storageHandler.GetLevelCount(); level++ {
		value, err := o.storageHandler.ReadBlock(level, int(request.Path), int(request.StorageId), offsetList[level])
		if err == nil {
			returnValue = value
		}
	}

	err := o.earlyReshuffle(int(request.Path), int(request.StorageId))
	if err != nil { // TODO: should we delete offsetList in case of an earlyReshuffle error?
		return nil, fmt.Errorf("early reshuffle failed;%s", err)
	}

	replicateDeleteOffsetListCommand, err := newReplicateDeleteOffsetListCommand(request.Block)
	if err != nil {
		return nil, fmt.Errorf("could not create delete offsetList replication command; %s", err)
	}
	err = o.raftNode.Apply(replicateDeleteOffsetListCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	o.readPathCounterMu.Lock()
	o.readPathCounter++
	o.readPathCounterMu.Unlock()

	return &pb.ReadPathReply{Value: returnValue}, nil
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
				oramNodeServer.evict(0, 0) // TODO: make it lexicographic
			}
		}
	}()
	go func() {
		for {
			oramNodeServer.performFailedEviction()
		}
	}()
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	pb.RegisterOramNodeServer(grpcServer, oramNodeServer)
	grpcServer.Serve(lis)
}
