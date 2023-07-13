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
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// How many ReadPath operations before eviction
const EvictionRate int = 4

type oramNodeServer struct {
	pb.UnimplementedOramNodeServer
	oramNodeServerID    int
	replicaID           int
	raftNode            *raft.Raft
	oramNodeFSM         *oramNodeFSM
	shardNodeRPCClients map[int]ReplicaRPCClientMap
	readPathCounter     int
	readPathCounterMu   sync.Mutex
	storageHandler      *storage.StorageHandler
}

func newOramNodeServer(oramNodeServerID int, replicaID int, raftNode *raft.Raft, oramNodeFSM *oramNodeFSM, shardNodeRPCClients map[int]ReplicaRPCClientMap, storageHandler *storage.StorageHandler) *oramNodeServer {
	return &oramNodeServer{
		oramNodeServerID:    oramNodeServerID,
		replicaID:           replicaID,
		raftNode:            raftNode,
		oramNodeFSM:         oramNodeFSM,
		shardNodeRPCClients: shardNodeRPCClients,
		readPathCounter:     0,
		storageHandler:      storageHandler,
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

func (o *oramNodeServer) evict(path int, storageID int) error {
	beginEvictionCommand, err := newReplicateBeginEvictionCommand(path, storageID)
	if err != nil {
		return fmt.Errorf("unable to marshal begin eviction command; %s", err)
	}
	err = o.raftNode.Apply(beginEvictionCommand, 2*time.Second).Error()
	if err != nil {
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	// TODO: General Notes from Our Discussions (Google Doc) - #6
	aggStash := make(map[string]string) //map of block to value
	randomShardNode := o.shardNodeRPCClients[0]
	recievedBlocksStatus := make(map[string]bool) // map of blocks to isWritten
	for level := 0; level < storage.LevelCount; level++ {
		blocks, err := o.storageHandler.ReadBucket(level, path, storageID)
		if err != nil {
			return fmt.Errorf("unable to read bucket; %s", err)
		}
		for block, value := range blocks {
			aggStash[block] = value
		}
		// TODO: get blocks from multiple random shard nodes
		shardNodeBlocks, err := randomShardNode.getBlocksFromShardNode(path, storageID)
		if err != nil {
			return fmt.Errorf("unable to get blocks from shard node; %s", err)
		}
		for _, block := range shardNodeBlocks {
			aggStash[block.Block] = block.Value
			recievedBlocksStatus[block.Block] = false
		}
	}

	for level := storage.LevelCount - 1; level >= 0; level-- {
		writtenBlocks, err := o.storageHandler.WriteBucket(level, path, storageID, aggStash, true)
		if err != nil {
			return fmt.Errorf("unable to atomic write bucket; %s", err)
		}
		for block := range writtenBlocks {
			delete(aggStash, block)
			recievedBlocksStatus[block] = true
		}
	}
	randomShardNode.sendBackAcksNacks(recievedBlocksStatus)

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

	requestID, err := rpc.GetRequestIDFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to read requestid from request; %s", err)
	}

	var offsetList []int
	for level := 0; level < storage.LevelCount; level++ {
		offset, err := o.storageHandler.GetBlockOffset(level, int(request.Path), int(request.StorageId), request.Block)
		if err != nil {
			return nil, fmt.Errorf("could not get offset from storage")
		}
		offsetList = append(offsetList, offset)
	}
	replicateOffsetAndBeginReadPathCommand, err := newReplicateOffsetListCommand(requestID, offsetList)
	if err != nil {
		return nil, fmt.Errorf("could not create offsetList replication command; %s", err)
	}
	err = o.raftNode.Apply(replicateOffsetAndBeginReadPathCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	var returnValue string
	for level := 0; level < storage.LevelCount; level++ {
		value, err := o.storageHandler.ReadBlock(level, int(request.Path), offsetList[level], int(request.StorageId))
		if err == nil {
			returnValue = value
		}
	}

	err = o.storageHandler.EarlyReshuffle(int(request.Path), int(request.StorageId))
	if err != nil { // TODO: should we delete offsetList in case of an earlyReshuffle error?
		return nil, fmt.Errorf("early reshuffle failed;%s", err)
	}

	replicateDeleteOffsetListCommand, err := newReplicateDeleteOffsetListCommand(requestID)
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

func StartServer(oramNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string, shardNodeRPCClients map[int]ReplicaRPCClientMap) {
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
	oramNodeServer := newOramNodeServer(oramNodeServerID, replicaID, r, oramNodeFSM, shardNodeRPCClients, storage.NewStorageHandler())
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			needEviction := false
			oramNodeServer.readPathCounterMu.Lock()
			if oramNodeServer.readPathCounter >= EvictionRate {
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
