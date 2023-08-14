package shardnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	pb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type shardNodeServer struct {
	pb.UnimplementedShardNodeServer
	shardNodeServerID int
	replicaID         int
	raftNode          *raft.Raft
	shardNodeFSM      *shardNodeFSM
	oramNodeClients   RPCClientMap
	storageHandler    *storage.StorageHandler
	batchManager      *batchManager
}

func newShardNodeServer(shardNodeServerID int, replicaID int, raftNode *raft.Raft, fsm *shardNodeFSM, oramNodeRPCClients RPCClientMap, storageHandler *storage.StorageHandler, batchManager *batchManager) *shardNodeServer {
	return &shardNodeServer{
		shardNodeServerID: shardNodeServerID,
		replicaID:         replicaID,
		raftNode:          raftNode,
		shardNodeFSM:      fsm,
		oramNodeClients:   oramNodeRPCClients,
		storageHandler:    storageHandler,
		batchManager:      batchManager,
	}
}

type OperationType int

const (
	Read = iota
	Write
)

// For the initial request of a block, it returns the path and storageID from the position map.
// For other requests, it returns a random path and storageID.
func (s *shardNodeServer) getPathAndStorageBasedOnRequest(block string, requestID string) (path int, storageID int) {
	if !s.shardNodeFSM.isInitialRequest(block, requestID) {
		return s.storageHandler.GetRandomPathAndStorageID()
	} else {
		s.shardNodeFSM.mu.Lock()
		defer s.shardNodeFSM.mu.Unlock()
		return s.shardNodeFSM.positionMap[block].path, s.shardNodeFSM.positionMap[block].storageID
	}
}

// It creates a channel for receiving the response from the raft FSM for the current requestID.
func (s *shardNodeServer) createResponseChannelForRequestID(requestID string) chan string {
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()
	ch := make(chan string)
	s.shardNodeFSM.responseChannel[requestID] = ch
	return ch
}

// It periodically sends batches.
func (s *shardNodeServer) sendBatchesForever() {
	for {
		s.sendCurrentBatches()
		time.Sleep(50 * time.Millisecond) // TODO: choose this based on the number of the requests
	}
}

// For queues that have more than batchSize requests, it sends the batch and waits for the responses.
func (s *shardNodeServer) sendCurrentBatches() {
	s.batchManager.mu.Lock()
	defer s.batchManager.mu.Unlock()
	for storageID, requests := range s.batchManager.storageQueues {
		if len(requests) >= s.batchManager.batchSize {
			oramNodeReplicaMap := s.oramNodeClients.getRandomOramNodeReplicaMap()
			reply, _ := oramNodeReplicaMap.readPathFromAllOramNodeReplicas(context.Background(), requests, storageID)
			for _, readPathReply := range reply.Responses {
				s.batchManager.responseChannel[readPathReply.Block] <- readPathReply.Value
			}
		}
	}
}

func (s *shardNodeServer) query(ctx context.Context, op OperationType, block string, value string) (string, error) {
	if s.raftNode.State() != raft.Leader {
		return "", fmt.Errorf("not the leader node")
	}
	requestID, err := rpc.GetRequestIDFromContext(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to read requestid from request; %s", err)
	}

	newPath, newStorageID := s.storageHandler.GetRandomPathAndStorageID()
	requestReplicationCommand, err := newRequestReplicationCommand(block, requestID, newPath, newStorageID)
	if err != nil {
		return "", fmt.Errorf("could not create request replication command; %s", err)
	}
	err = s.raftNode.Apply(requestReplicationCommand, 2*time.Second).Error()
	if err != nil {
		return "", fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	path, storageID := s.getPathAndStorageBasedOnRequest(block, requestID)

	isInStash := false
	s.shardNodeFSM.mu.Lock()
	if _, exists := s.shardNodeFSM.stash[block]; exists {
		isInStash = true
	}
	s.shardNodeFSM.mu.Unlock()

	var replyValue string
	if isInStash || !s.shardNodeFSM.isInitialRequest(block, requestID) {
		s.batchManager.addRequestToStorageQueueWithoutWaiting(blockRequest{block: block, path: path}, storageID)
	} else {
		oramReplyChan := s.batchManager.addRequestToStorageQueueAndWait(blockRequest{block: block, path: path}, storageID)
		replyValue = <-oramReplyChan
	}

	responseChannel := s.createResponseChannelForRequestID(requestID)
	if s.shardNodeFSM.isInitialRequest(block, requestID) {
		responseReplicationCommand, err := newResponseReplicationCommand(replyValue, requestID, block, value, op)
		if err != nil {
			return "", fmt.Errorf("could not create response replication command; %s", err)
		}
		err = s.raftNode.Apply(responseReplicationCommand, 2*time.Second).Error()
		if err != nil {
			return "", fmt.Errorf("could not apply log to the FSM; %s", err)
		}
	}
	responseValue := <-responseChannel
	return responseValue, nil
}

func (s *shardNodeServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	fmt.Println("Read on shard node is called")
	val, err := s.query(ctx, Read, readRequest.Block, "")
	if err != nil {
		return nil, err
	}
	return &pb.ReadReply{Value: val}, nil
}

func (s *shardNodeServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	fmt.Println("Write on shard node is called")
	val, err := s.query(ctx, Write, writeRequest.Block, writeRequest.Value)
	if err != nil {
		return nil, err
	}
	return &pb.WriteReply{Success: val == writeRequest.Value}, nil
}

// It gets maxBlocks from the stash to send to the requesting oram node.
// The blocks should be for the same path and storageID.
func (s *shardNodeServer) getBlocksForSend(maxBlocks int, paths []int, storageID int) (blocksToReturn []*pb.Block, blocks []string) {
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()

	counter := 0
	for block, stashState := range s.shardNodeFSM.stash {
		if (!s.shardNodeFSM.positionMap[block].isPathForPaths(paths)) || (s.shardNodeFSM.positionMap[block].storageID != storageID) {
			continue
		}

		// Don't send a stash block that is in the waiting status to another SendBlocks request
		if stashState.waitingStatus {
			continue
		}
		if counter == int(maxBlocks) {
			break
		}
		blocksToReturn = append(blocksToReturn, &pb.Block{Block: block, Value: stashState.value})
		blocks = append(blocks, block)
		counter++

	}
	return blocksToReturn, blocks
}

// It sends blocks to the oram node for eviction.
func (s *shardNodeServer) SendBlocks(ctx context.Context, request *pb.SendBlocksRequest) (*pb.SendBlocksReply, error) {

	blocksToReturn, blocks := s.getBlocksForSend(int(request.MaxBlocks), utils.ConvertInt32SliceToIntSlice(request.Paths), int(request.StorageId))

	sentBlocksReplicationCommand, err := newSentBlocksReplicationCommand(blocks)
	if err != nil {
		return nil, fmt.Errorf("could not create sent blocks replication command; %s", err)
	}
	err = s.raftNode.Apply(sentBlocksReplicationCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	return &pb.SendBlocksReply{Blocks: blocksToReturn}, nil
}

// It gets the acks and nacks from the oram node.
// Ackes and Nackes get replicated to be handled in the raft layer.
func (s *shardNodeServer) AckSentBlocks(ctx context.Context, reply *pb.AckSentBlocksRequest) (*pb.AckSentBlocksReply, error) {
	var ackedBlocks []string
	var nackedBlocks []string
	for _, ack := range reply.Acks {
		block := ack.Block
		is_ack := ack.IsAck
		if is_ack {
			ackedBlocks = append(ackedBlocks, block)
		} else {
			nackedBlocks = append(nackedBlocks, block)
		}
	}

	acksNacksReplicationCommand, err := newAcksNacksReplicationCommand(ackedBlocks, nackedBlocks)
	if err != nil {
		return nil, fmt.Errorf("could not create acks/nacks replication command")
	}
	err = s.raftNode.Apply(acksNacksReplicationCommand, 1*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	return &pb.AckSentBlocksReply{Success: true}, nil
}

func (s *shardNodeServer) JoinRaftVoter(ctx context.Context, joinRaftVoterRequest *pb.JoinRaftVoterRequest) (*pb.JoinRaftVoterReply, error) {
	requestingNodeId := joinRaftVoterRequest.NodeId
	requestingNodeAddr := joinRaftVoterRequest.NodeAddr

	log.Printf("received join request from node %d at %s", requestingNodeId, requestingNodeAddr)

	err := s.raftNode.AddVoter(
		raft.ServerID(strconv.Itoa(int(requestingNodeId))),
		raft.ServerAddress(requestingNodeAddr),
		0, 0).Error()

	if err != nil {
		return &pb.JoinRaftVoterReply{Success: false}, fmt.Errorf("voter could not be added to the leader; %s", err)
	}
	return &pb.JoinRaftVoterReply{Success: true}, nil
}

func StartServer(shardNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string, oramNodeRPCClients map[int]ReplicaRPCClientMap, parameters config.Parameters) {
	isFirst := joinAddr == ""
	shardNodeFSM := newShardNodeFSM()
	r, err := startRaftServer(isFirst, replicaID, raftPort, shardNodeFSM)
	if err != nil {
		log.Fatalf("The raft node creation did not succeed; %s", err)
	}
	shardNodeFSM.mu.Lock()
	shardNodeFSM.raftNode = r
	shardNodeFSM.mu.Unlock()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			fmt.Println(shardNodeFSM)
		}
	}()

	if !isFirst {
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("The raft node could not connect to the leader as a new voter; %s", err)
		}
		client := pb.NewShardNodeClient(conn)
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
	shardnodeServer := newShardNodeServer(shardNodeServerID, replicaID, r, shardNodeFSM, oramNodeRPCClients, storage.NewStorageHandler(), newBatchManager(parameters.BatchSize))
	go shardnodeServer.sendBatchesForever()

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	pb.RegisterShardNodeServer(grpcServer, shardnodeServer)
	grpcServer.Serve(lis)
}
