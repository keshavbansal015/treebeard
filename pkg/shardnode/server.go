package shardnode

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	pb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
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
// For other requests, it returns a random path and storageID and a random block.
func (s *shardNodeServer) getWhatToSendBasedOnRequest(ctx context.Context, block string, requestID string) (blockToRequest string, path int, storageID int) {
	log.Debug().Msgf("Getting path and storageID based on request for block %s and requestID %s", block, requestID)
	if !s.shardNodeFSM.isInitialRequest(block, requestID) {
		path, storageID = s.storageHandler.GetRandomPathAndStorageID(ctx)
		return strconv.Itoa(rand.Int()), path, storageID
	} else {
		s.shardNodeFSM.mu.Lock()
		defer s.shardNodeFSM.mu.Unlock()
		return block, s.shardNodeFSM.positionMap[block].path, s.shardNodeFSM.positionMap[block].storageID
	}
}

// It creates a channel for receiving the response from the raft FSM for the current requestID.
func (s *shardNodeServer) createResponseChannelForRequestID(requestID string) chan string {
	log.Debug().Msgf("Aquiring lock for shard node FSM in createResponseChannelForRequestID")
	s.shardNodeFSM.mu.Lock()
	log.Debug().Msgf("Aquired lock for shard node FSM in createResponseChannelForRequestID")
	defer func() {
		log.Debug().Msgf("Releasing lock for shard node FSM in createResponseChannelForRequestID")
		s.shardNodeFSM.mu.Unlock()
		log.Debug().Msgf("Released lock for shard node FSM in createResponseChannelForRequestID")
	}()
	ch := make(chan string)
	s.shardNodeFSM.responseChannel[requestID] = ch
	return ch
}

// It periodically sends batches.
func (s *shardNodeServer) sendBatchesForever() {
	for {
		s.sendCurrentBatches()
	}
}

// For queues that have equal or more than batchSize requests, it sends a batch of size=BatchSize and waits for the responses.
// The logic here assumes that there are no duplicate blocks in the requests (which is fine since we only send a real request for the first one).
// It will not work otherwise because it will delete the response channel for a block after getting the first response.
func (s *shardNodeServer) sendCurrentBatches() {
	s.batchManager.mu.Lock() // TODO: don't lock the whole thing, it will prevent concurrent batch sends
	defer s.batchManager.mu.Unlock()
	for storageID, requests := range s.batchManager.storageQueues {
		if len(requests) >= s.batchManager.batchSize {
			oramNodeReplicaMap := s.oramNodeClients.getRandomOramNodeReplicaMap()
			log.Debug().Msgf("Sending batch of size %d to storageID %d", s.batchManager.batchSize, storageID)
			reply, err := oramNodeReplicaMap.readPathFromAllOramNodeReplicas(requests[0].ctx, requests[0:s.batchManager.batchSize], storageID)
			s.batchManager.deleteRequestsFromQueue(storageID, s.batchManager.batchSize)
			if err != nil {
				log.Error().Msgf("Could not get value from the oramnode; %s", err)
				continue
				// TOOD: think about the case where the oram node doesn't return a response in 2 seconds
			}
			for _, readPathReply := range reply.Responses {
				log.Debug().Msgf("Got reply from oram node replica: %v", readPathReply)
				if _, exists := s.batchManager.responseChannel[readPathReply.Block]; exists {
					s.batchManager.responseChannel[readPathReply.Block] <- readPathReply.Value
					delete(s.batchManager.responseChannel, readPathReply.Block)
				}
			}
		}
	}
}

func (s *shardNodeServer) query(ctx context.Context, op OperationType, block string, value string) (string, error) {
	if s.raftNode.State() != raft.Leader {
		return "", fmt.Errorf("not the leader node")
	}
	tracer := otel.Tracer("")
	ctx, querySpan := tracer.Start(ctx, "shardnode query")
	requestID, err := rpc.GetRequestIDFromContext(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to read requestid from request; %s", err)
	}

	newPath, newStorageID := s.storageHandler.GetRandomPathAndStorageID(ctx)
	requestReplicationCommand, err := newRequestReplicationCommand(block, requestID, newPath, newStorageID)
	if err != nil {
		return "", fmt.Errorf("could not create request replication command; %s", err)
	}
	_, requestReplicationSpan := tracer.Start(ctx, "apply request replication")
	err = s.raftNode.Apply(requestReplicationCommand, 2*time.Second).Error()
	requestReplicationSpan.End()
	if err != nil {
		return "", fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	blockToRequest, path, storageID := s.getWhatToSendBasedOnRequest(ctx, block, requestID)

	var replyValue string
	_, waitOnReplySpan := tracer.Start(ctx, "wait on reply")

	log.Debug().Msgf("Adding request to storage queue and waiting for block %s", block)
	oramReplyChan := s.batchManager.addRequestToStorageQueueAndWait(blockRequest{ctx: ctx, block: blockToRequest, path: path}, storageID)
	replyValue = <-oramReplyChan

	waitOnReplySpan.End()
	responseChannel := s.createResponseChannelForRequestID(requestID)
	if s.shardNodeFSM.isInitialRequest(block, requestID) {
		log.Debug().Msgf("Adding response to response channel for block %s", block)
		responseReplicationCommand, err := newResponseReplicationCommand(replyValue, requestID, block, value, op)
		if err != nil {
			return "", fmt.Errorf("could not create response replication command; %s", err)
		}
		_, responseReplicationSpan := tracer.Start(ctx, "apply response replication")
		err = s.raftNode.Apply(responseReplicationCommand, 2*time.Second).Error()
		responseReplicationSpan.End()
		if err != nil {
			return "", fmt.Errorf("could not apply log to the FSM; %s", err)
		}
	}
	responseValue := <-responseChannel
	log.Debug().Msgf("Got response from response channel for block %s; value: %s", block, responseValue)
	querySpan.End()
	return responseValue, nil
}

func (s *shardNodeServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	log.Debug().Msgf("Received read request for block %s", readRequest.Block)
	val, err := s.query(ctx, Read, readRequest.Block, "")
	if err != nil {
		return nil, err
	}
	return &pb.ReadReply{Value: val}, nil
}

func (s *shardNodeServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	log.Debug().Msgf("Received write request for block %s", writeRequest.Block)
	val, err := s.query(ctx, Write, writeRequest.Block, writeRequest.Value)
	if err != nil {
		return nil, err
	}
	return &pb.WriteReply{Success: val == writeRequest.Value}, nil
}

// It gets maxBlocks from the stash to send to the requesting oram node.
// The blocks should be for the same path and storageID.
func (s *shardNodeServer) getBlocksForSend(maxBlocks int, paths []int, storageID int) (blocksToReturn []*pb.Block, blocks []string) {
	log.Debug().Msgf("Aquiring lock for shard node FSM in getBlocksForSend")
	s.shardNodeFSM.mu.Lock()
	log.Debug().Msgf("Aquired lock for shard node FSM in getBlocksForSend")
	defer func() {
		log.Debug().Msgf("Releasing lock for shard node FSM in getBlocksForSend")
		s.shardNodeFSM.mu.Unlock()
		log.Debug().Msgf("Released lock for shard node FSM in getBlocksForSend")
	}()

	counter := 0
	for block, stashState := range s.shardNodeFSM.stash {
		if (!s.shardNodeFSM.positionMap[block].isPathInPaths(paths)) || (s.shardNodeFSM.positionMap[block].storageID != storageID) {
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
	log.Debug().Msgf("Sending blocks %v for paths %v and storageID %d", blocks, paths, storageID)
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
	log.Debug().Msgf("Received acks %v and nacks %v", ackedBlocks, nackedBlocks)

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

func StartServer(shardNodeServerID int, ip string, rpcPort int, replicaID int, raftPort int, raftDir string, joinAddr string, oramNodeRPCClients map[int]ReplicaRPCClientMap, parameters config.Parameters, configsPath string) {
	isFirst := joinAddr == ""
	shardNodeFSM := newShardNodeFSM()
	r, err := startRaftServer(isFirst, ip, replicaID, raftPort, raftDir, shardNodeFSM)
	if err != nil {
		log.Fatal().Msgf("The raft node creation did not succeed; %s", err)
	}
	shardNodeFSM.mu.Lock()
	shardNodeFSM.raftNode = r
	shardNodeFSM.mu.Unlock()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			log.Debug().Msgf(shardNodeFSM.String())
		}
	}()

	if !isFirst {
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal().Msgf("The raft node could not connect to the leader as a new voter; %s", err)
		}
		client := pb.NewShardNodeClient(conn)
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
	storageHandler := storage.NewStorageHandler()
	if isFirst {
		storageHandler.InitDatabase(configsPath)
	}
	shardnodeServer := newShardNodeServer(shardNodeServerID, replicaID, r, shardNodeFSM, oramNodeRPCClients, storageHandler, newBatchManager(parameters.BatchSize))
	go shardnodeServer.sendBatchesForever()

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	pb.RegisterShardNodeServer(grpcServer, shardnodeServer)
	grpcServer.Serve(lis)
}
