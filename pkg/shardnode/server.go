package shardnode

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/keshavbansal015/treebeard/api/shardnode"
	"github.com/keshavbansal015/treebeard/pkg/commonerrs"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/rpc"
	"github.com/keshavbansal015/treebeard/pkg/storage"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type shardNodeServer struct {
	pb.UnimplementedShardNodeServer
	shardNodeServerID  int
	replicaID          int
	raftNode           *raft.Raft
	shardNodeFSM       *shardNodeFSM
	oramNodeClients    RPCClientMap
	storageORAMNodeMap map[int]int // map of storageID to responsible oramNodeID
	storageTreeHeight  int
	batchManager       *batchManager
}

func newShardNodeServer(shardNodeServerID int, replicaID int, raftNode *raft.Raft, fsm *shardNodeFSM, oramNodeRPCClients RPCClientMap, storageORAMNodeMap map[int]int, storageTreeHeight int, batchManager *batchManager) *shardNodeServer {
	log.Debug().Msgf("Initializing Shard Node Server with ID %d and Replica ID %d", shardNodeServerID, replicaID)
	return &shardNodeServer{
		shardNodeServerID:  shardNodeServerID,
		replicaID:          replicaID,
		raftNode:           raftNode,
		shardNodeFSM:       fsm,
		oramNodeClients:    oramNodeRPCClients,
		batchManager:       batchManager,
		storageORAMNodeMap: storageORAMNodeMap,
		storageTreeHeight:  storageTreeHeight,
	}
}

type OperationType int

const (
	Read = iota
	Write
)

// For the initial request of a block, it returns the path and storageID from the position map.
// For other requests, it returns a random path and storageID and a random block.
func (s *shardNodeServer) getWhatToSendBasedOnRequest(ctx context.Context, block string, requestID string, isFirst bool) (blockToRequest string, path int, storageID int) {
	log.Debug().Msgf("Getting path and storageID for requestID %s, block %s. Is this the first request? %v", requestID, block, isFirst)
	if !isFirst {
		path, storageID = storage.GetRandomPathAndStorageID(s.storageTreeHeight, len(s.storageORAMNodeMap))
		log.Debug().Msgf("This is not the first request. Returning random block and path: %s, %d, %d", block+strconv.Itoa(rand.Int()), path, storageID)
		return block + strconv.Itoa(rand.Int()), path, storageID
	} else {
		s.shardNodeFSM.positionMapMu.RLock()
		defer s.shardNodeFSM.positionMapMu.RUnlock()
		if _, exists := s.shardNodeFSM.positionMap[block]; !exists {
			path, storageID = storage.GetRandomPathAndStorageID(s.storageTreeHeight, len(s.storageORAMNodeMap))
			log.Debug().Msgf("Block %s not found in position map. Returning random path and storageID: %d, %d", block, path, storageID)
			return block, path, storageID
		} else {
			path = s.shardNodeFSM.positionMap[block].path
			storageID = s.shardNodeFSM.positionMap[block].storageID
			log.Debug().Msgf("Block %s found in position map. Returning its path and storageID: %d, %d", block, path, storageID)
			return block, path, storageID
		}
	}
}

// It creates a channel for receiving the response from the raft FSM for the current requestID.
// The response channel should be buffered so that we don't block the raft FSM even if the client is not reading from the channel right now.
func (s *shardNodeServer) createResponseChannelForBatch(readRequests []*pb.ReadRequest, writeRequests []*pb.WriteRequest) map[string]chan string {
	log.Debug().Msgf("Creating response channels for a batch with %d read requests and %d write requests", len(readRequests), len(writeRequests))
	channelMap := make(map[string]chan string)
	for _, req := range readRequests {
		channelMap[req.RequestId] = make(chan string, 1)
		s.shardNodeFSM.responseChannel.Store(req.RequestId, channelMap[req.RequestId])
		log.Debug().Msgf("Created response channel for read request ID %s", req.RequestId)
	}
	for _, req := range writeRequests {
		channelMap[req.RequestId] = make(chan string, 1)
		s.shardNodeFSM.responseChannel.Store(req.RequestId, channelMap[req.RequestId])
		log.Debug().Msgf("Created response channel for write request ID %s", req.RequestId)
	}
	log.Debug().Msgf("Total response channels created: %d", len(channelMap))
	return channelMap
}

// It periodically sends batches.
func (s *shardNodeServer) sendBatchesForever() {
	log.Info().Msg("Starting batch sender goroutine.")
	for {
		log.Debug().Msgf("Waiting for batch timeout of %s", s.batchManager.batchTimeout)
		<-time.After(s.batchManager.batchTimeout)
		log.Debug().Msg("Batch timeout reached. Sending current batches.")
		s.sendCurrentBatches()
	}
}

// For queues that have equal or more than batchSize requests, it sends a batch of size=BatchSize and waits for the responses.
// The logic here assumes that there are no duplicate blocks in the requests (which is fine since we only send a real request for the first one).
// It will not work otherwise because it will delete the response channel for a block after getting the first response.
func (s *shardNodeServer) sendCurrentBatches() {
	log.Info().Msg("Starting sendCurrentBatches.")
	storageQueues := make(map[int][]blockRequest)
	responseChannels := make(map[string]chan string)

	s.batchManager.mu.HighPriorityLock()
	log.Debug().Msg("Acquired high priority lock for batch manager.")
	for storageID, requests := range s.batchManager.storageQueues {
		log.Debug().Msgf("Moving %d requests from storage ID %d to be processed", len(requests), storageID)
		storageQueues[storageID] = append(storageQueues[storageID], requests...)
		delete(s.batchManager.storageQueues, storageID)
	}
	for block, responseChannel := range s.batchManager.responseChannel {
		responseChannels[block] = responseChannel
		log.Debug().Msgf("Moving response channel for block %s", block)
		delete(s.batchManager.responseChannel, block)
	}
	s.batchManager.mu.HighPriorityUnlock()
	log.Debug().Msg("Released high priority lock.")

	batchRequestResponseChan := make(chan batchResponse)
	waitingBatchCount := 0
	for storageID, requests := range storageQueues {
		if len(requests) == 0 {
			continue
		}
		waitingBatchCount++
		log.Debug().Msgf("Dispatching a batch of %d requests for storageID %d", len(requests), storageID)
		oramNodeReplicaMap := s.oramNodeClients[s.storageORAMNodeMap[storageID]]
		go s.batchManager.asyncBatchRequests(context.Background(), storageID, requests, oramNodeReplicaMap, batchRequestResponseChan)
	}
	log.Debug().Msgf("Waiting for %d batch responses", waitingBatchCount)

	for i := 0; i < waitingBatchCount; i++ {
		response := <-batchRequestResponseChan
		if response.err != nil {
			log.Error().Msgf("Could not get value from the oramnode; %s", response.err)
			continue
		}
		log.Debug().Msgf("Got batch response from oram node replica. It contains %d replies.", len(response.Responses))
		go func(response batchResponse) {
			for _, readPathReply := range response.Responses {
				log.Debug().Msgf("Processing reply for block %s", readPathReply.Block)
				if _, exists := responseChannels[readPathReply.Block]; exists {
					timeout := time.After(1 * time.Second)
					select {
					case <-timeout:
						log.Error().Msgf("Timeout while waiting for batch response channel for block %s", readPathReply.Block)
						continue
					case responseChannels[readPathReply.Block] <- readPathReply.Value:
						log.Debug().Msgf("Sent value for block %s to response channel", readPathReply.Block)
					}
				} else {
					log.Debug().Msgf("No response channel exists for block %s. Discarding reply.", readPathReply.Block)
				}
			}
		}(response)
	}
	log.Info().Msg("sendCurrentBatches completed.")
}

func (s *shardNodeServer) getRequestReplicationBlocks(readRequests []*pb.ReadRequest, writeRequests []*pb.WriteRequest) (requestReplicationBlocks []ReplicateRequestAndPathAndStoragePayload) {
	log.Debug().Msgf("Preparing replication blocks for %d read and %d write requests", len(readRequests), len(writeRequests))
	for _, readRequest := range readRequests {
		newPath, newStorageID := storage.GetRandomPathAndStorageID(s.storageTreeHeight, len(s.storageORAMNodeMap))
		requestReplicationBlocks = append(requestReplicationBlocks, ReplicateRequestAndPathAndStoragePayload{
			RequestedBlock: readRequest.Block,
			RequestID:      readRequest.RequestId,
			Path:           newPath,
			StorageID:      newStorageID,
		})
		log.Trace().Msgf("Read request %s: assigned new path %d, storageID %d", readRequest.RequestId, newPath, newStorageID)
	}
	for _, writeRequest := range writeRequests {
		newPath, newStorageID := storage.GetRandomPathAndStorageID(s.storageTreeHeight, len(s.storageORAMNodeMap))
		requestReplicationBlocks = append(requestReplicationBlocks, ReplicateRequestAndPathAndStoragePayload{
			RequestedBlock: writeRequest.Block,
			RequestID:      writeRequest.RequestId,
			Path:           newPath,
			StorageID:      newStorageID,
		})
		log.Trace().Msgf("Write request %s: assigned new path %d, storageID %d", writeRequest.RequestId, newPath, newStorageID)
	}
	log.Debug().Msgf("Prepared %d replication blocks in total", len(requestReplicationBlocks))
	return requestReplicationBlocks
}

type finalResponse struct {
	requestId string
	value     string
	opType    OperationType
	err       error
}

func (s *shardNodeServer) query(ctx context.Context, block string, requestID string, isFirst bool, newVal string, opType OperationType, raftResponseChannel chan string, finalResponseChannel chan finalResponse) {
	tracer := otel.Tracer("")
	log.Debug().Msgf("Executing query for request %s, block %s. isFirst: %v, operation: %d", requestID, block, isFirst, opType)

	blockToRequest, path, storageID := s.getWhatToSendBasedOnRequest(ctx, block, requestID, isFirst)
	var replyValue string
	_, waitOnReplySpan := tracer.Start(ctx, "wait on reply")
	log.Debug().Msgf("Adding request for block %s to storage queue for storageID %d and path %d", blockToRequest, storageID, path)
	oramReplyChan := s.batchManager.addRequestToStorageQueueAndWait(blockRequest{ctx: ctx, block: blockToRequest, path: path}, storageID)
	log.Debug().Msgf("Waiting for reply from ORAM node on channel for block %s", blockToRequest)
	replyValue = <-oramReplyChan
	log.Debug().Msgf("Got reply from oram node channel for block %s; value: %s", blockToRequest, replyValue)
	waitOnReplySpan.End()

	if isFirst {
		log.Debug().Msgf("This is the first request for block %s. Replicating response and position map changes.", block)
		responseReplicationCommand, err := newResponseReplicationCommand(replyValue, requestID, block, newVal, opType, s.replicaID)
		if err != nil {
			log.Error().Msgf("Could not create response replication command for request %s: %s", requestID, err)
			finalResponseChannel <- finalResponse{requestId: requestID, value: "", opType: opType, err: fmt.Errorf("could not create response replication command; %s", err)}
			return
		}
		_, responseReplicationSpan := tracer.Start(ctx, "apply response replication")
		log.Debug().Msgf("Applying response replication command for request %s", requestID)
		responseApplyFuture := s.raftNode.Apply(responseReplicationCommand, 0)
		err = responseApplyFuture.Error()
		responseReplicationSpan.End()
		if err != nil {
			log.Error().Msgf("Could not apply log to the FSM for request %s: %s", requestID, err)
			finalResponseChannel <- finalResponse{requestId: requestID, value: "", opType: opType, err: fmt.Errorf("could not apply log to the FSM; %s", err)}
			return
		}
		response := responseApplyFuture.Response().(string)
		log.Debug().Msgf("Got FSM response for request %s; value: %s", requestID, response)
		finalResponseChannel <- finalResponse{requestId: requestID, value: response, opType: opType, err: nil}
		return
	}
	log.Debug().Msgf("This is a non-first request. Waiting for response from Raft FSM for request %s", requestID)
	responseValue := <-raftResponseChannel
	log.Debug().Msgf("Got final response from Raft FSM for request %s; value: %s", requestID, responseValue)
	finalResponseChannel <- finalResponse{requestId: requestID, value: responseValue, opType: opType, err: nil}
}

func (s *shardNodeServer) queryBatch(ctx context.Context, request *pb.RequestBatch) (reply *pb.ReplyBatch, err error) {
	if s.raftNode.State() != raft.Leader {
		log.Warn().Msgf("Received batch query but this node is not the leader. State: %s", s.raftNode.State().String())
		return nil, fmt.Errorf(commonerrs.NotTheLeaderError)
	}
	log.Info().Msgf("Received batch query with %d read and %d write requests", len(request.ReadRequests), len(request.WriteRequests))
	tracer := otel.Tracer("")
	ctx, querySpan := tracer.Start(ctx, "shardnode query")
	defer querySpan.End()

	responseChannel := s.createResponseChannelForBatch(request.ReadRequests, request.WriteRequests)
	requestReplicationBlocks := s.getRequestReplicationBlocks(request.ReadRequests, request.WriteRequests)
	requestReplicationCommand, err := newRequestReplicationCommand(requestReplicationBlocks, s.replicaID)
	if err != nil {
		log.Error().Msgf("Could not create request replication command: %s", err)
		return nil, fmt.Errorf("could not create request replication command; %s", err)
	}
	_, requestReplicationSpan := tracer.Start(ctx, "apply request replication")
	log.Debug().Msg("Applying request replication command to Raft FSM")
	requestApplyFuture := s.raftNode.Apply(requestReplicationCommand, 0)
	err = requestApplyFuture.Error()
	requestReplicationSpan.End()
	if err != nil {
		log.Error().Msgf("Could not apply log to the FSM: %s", err)
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	isFirstMap := requestApplyFuture.Response().(map[string]bool)
	log.Debug().Msgf("Received isFirstMap from FSM: %v", isFirstMap)

	finalResponseChan := make(chan finalResponse)
	// finalResponseChan := make(chan finalResponse, len(request.ReadRequests)+len(request.WriteRequests))
	for _, readRequest := range request.ReadRequests {
		go s.query(ctx, readRequest.Block, readRequest.RequestId, isFirstMap[readRequest.RequestId], "", Read, responseChannel[readRequest.RequestId], finalResponseChan)
	}
	for _, writeRequest := range request.WriteRequests {
		go s.query(ctx, writeRequest.Block, writeRequest.RequestId, isFirstMap[writeRequest.RequestId], writeRequest.Value, Write, responseChannel[writeRequest.RequestId], finalResponseChan)
	}

	var readReplies []*pb.ReadReply
	var writeReplies []*pb.WriteReply
	totalRequests := len(request.ReadRequests) + len(request.WriteRequests)
	log.Debug().Msgf("Waiting for %d final responses", totalRequests)
	for i := 0; i < totalRequests; i++ {
		response := <-finalResponseChan
		if response.err != nil {
			log.Error().Msgf("Error in final response for request %s: %s", response.requestId, response.err)
			return nil, fmt.Errorf("could not get response from the oramnode; %s", response.err)
		}
		if response.opType == Read {
			readReplies = append(readReplies, &pb.ReadReply{RequestId: response.requestId, Value: response.value})
			log.Debug().Msgf("Got read reply for request %s", response.requestId)
		} else {
			writeReplies = append(writeReplies, &pb.WriteReply{RequestId: response.requestId, Success: true})
			log.Debug().Msgf("Got write reply for request %s", response.requestId)
		}
	}
	querySpan.End()
	log.Info().Msg("Batch query completed successfully")
	return &pb.ReplyBatch{ReadReplies: readReplies, WriteReplies: writeReplies}, nil
}

func (s *shardNodeServer) BatchQuery(ctx context.Context, request *pb.RequestBatch) (*pb.ReplyBatch, error) {
	log.Info().Msgf("Received request batch containing %d read and %d write requests", len(request.ReadRequests), len(request.WriteRequests))
	reply, err := s.queryBatch(ctx, request)
	if err != nil {
		log.Error().Msgf("Batch query failed: %v", err)
		return nil, err
	}
	log.Info().Msgf("Returning batch reply with %d read and %d write replies", len(reply.ReadReplies), len(reply.WriteReplies))
	log.Debug().Msgf("Reply details: %v", reply)
	return reply, nil
}

// It gets maxBlocks from the stash to send to the requesting oram node.
// The blocks should be for the same path and storageID.
func (s *shardNodeServer) getBlocksForSend(maxBlocks int, storageID int) (blocksToReturn []*pb.Block, blocks []string) {
	log.Debug().Msgf("Getting up to %d blocks for storageID %d to send for eviction", maxBlocks, storageID)
	log.Debug().Msgf("Acquiring lock for shard node FSM in getBlocksForSend")
	s.shardNodeFSM.stashMu.Lock()
	s.shardNodeFSM.positionMapMu.RLock()
	log.Debug().Msgf("Acquired lock for shard node FSM in getBlocksForSend")
	defer func() {
		log.Debug().Msgf("Releasing lock for shard node FSM in getBlocksForSend")
		s.shardNodeFSM.stashMu.Unlock()
		s.shardNodeFSM.positionMapMu.RUnlock()
		log.Debug().Msgf("Released lock for shard node FSM in getBlocksForSend")
	}()

	counter := 0
	for block, stashState := range s.shardNodeFSM.stash {
		if counter == int(maxBlocks) {
			break
		}
		blocksToReturn = append(blocksToReturn, &pb.Block{Block: block, Value: stashState.value, Path: int32(s.shardNodeFSM.positionMap[block].path)})
		blocks = append(blocks, block)
		counter++
		log.Trace().Msgf("Selected block %s from stash for path %d", block, s.shardNodeFSM.positionMap[block].path)
	}
	log.Debug().Msgf("Selected %d blocks to send for storageID %d", len(blocks), storageID)
	return blocksToReturn, blocks
}

// It sends blocks to the oram node for eviction.
func (s *shardNodeServer) SendBlocks(ctx context.Context, request *pb.SendBlocksRequest) (*pb.SendBlocksReply, error) {
	log.Info().Msgf("Received SendBlocks request for storageID %d, max blocks %d", request.StorageId, request.MaxBlocks)
	blocksToReturn, blocks := s.getBlocksForSend(int(request.MaxBlocks), int(request.StorageId))

	sentBlocksReplicationCommand, err := newSentBlocksReplicationCommand(blocks)
	if err != nil {
		log.Error().Msgf("Could not create sent blocks replication command: %s", err)
		return nil, fmt.Errorf("could not create sent blocks replication command; %s", err)
	}
	log.Debug().Msgf("Applying sent blocks replication command for %d blocks", len(blocks))
	err = s.raftNode.Apply(sentBlocksReplicationCommand, 0).Error()
	if err != nil {
		log.Error().Msgf("Could not apply log to the FSM: %s", err)
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	log.Info().Msgf("Successfully sent %d blocks", len(blocksToReturn))
	return &pb.SendBlocksReply{Blocks: blocksToReturn}, nil
}

// It gets the acks and nacks from the oram node.
// Ackes and Nackes get replicated to be handled in the raft layer.
func (s *shardNodeServer) AckSentBlocks(ctx context.Context, reply *pb.AckSentBlocksRequest) (*pb.AckSentBlocksReply, error) {
	log.Info().Msgf("Received AckSentBlocks request with %d acks/nacks", len(reply.Acks))
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
	log.Debug().Msgf("Received %d acks: %v", len(ackedBlocks), ackedBlocks)
	log.Debug().Msgf("Received %d nacks: %v", len(nackedBlocks), nackedBlocks)

	acksNacksReplicationCommand, err := newAcksNacksReplicationCommand(ackedBlocks, nackedBlocks)
	if err != nil {
		log.Error().Msgf("Could not create acks/nacks replication command: %s", err)
		return nil, fmt.Errorf("could not create acks/nacks replication command")
	}
	log.Debug().Msg("Applying acks/nacks replication command to Raft FSM")
	err = s.raftNode.Apply(acksNacksReplicationCommand, 0).Error()
	if err != nil {
		log.Error().Msgf("Could not apply log to the FSM: %s", err)
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	log.Info().Msg("Successfully processed acks and nacks")
	return &pb.AckSentBlocksReply{Success: true}, nil
}

func (s *shardNodeServer) JoinRaftVoter(ctx context.Context, joinRaftVoterRequest *pb.JoinRaftVoterRequest) (*pb.JoinRaftVoterReply, error) {
	requestingNodeId := joinRaftVoterRequest.NodeId
	requestingNodeAddr := joinRaftVoterRequest.NodeAddr

	log.Printf("received join request from node %d at %s", requestingNodeId, requestingNodeAddr)
	log.Debug().Msgf("Attempting to add voter: NodeID=%d, Address=%s", requestingNodeId, requestingNodeAddr)

	err := s.raftNode.AddVoter(
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

func StartServer(shardNodeServerID int, bindIp string, advertiseIp string, rpcPort int, replicaID int, raftPort int, joinAddr string, oramNodeRPCClients map[int]ReplicaRPCClientMap, parameters config.Parameters, storages []config.RedisEndpoint, configsPath string) {
	log.Info().Msg("Starting Shard Node Server")
	isFirst := joinAddr == ""
	log.Debug().Msgf("Is this the first node in the cluster? %v", isFirst)
	shardNodeFSM := newShardNodeFSM(replicaID)
	r, err := startRaftServer(isFirst, bindIp, advertiseIp, replicaID, raftPort, shardNodeFSM)
	if err != nil {
		log.Fatal().Msgf("The raft node creation did not succeed: %s", err)
	}

	if !isFirst {
		log.Info().Msgf("This is not the first node. Attempting to join the cluster at %s", joinAddr)
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal().Msgf("The raft node could not connect to the leader as a new voter: %s", err)
		}
		client := pb.NewShardNodeClient(conn)
		log.Debug().Msgf("Sending join request to leader at %s", joinAddr)
		joinRaftVoterReply, err := client.JoinRaftVoter(
			context.Background(),
			&pb.JoinRaftVoterRequest{
				NodeId:   int32(replicaID),
				NodeAddr: fmt.Sprintf("%s:%d", advertiseIp, raftPort),
			},
		)
		if err != nil || !joinRaftVoterReply.Success {
			log.Error().Msgf("The raft node could not connect to the leader as a new voter: %s", err)
		} else {
			log.Info().Msg("Successfully joined the raft cluster.")
		}
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindIp, rpcPort))
	if err != nil {
		log.Fatal().Msgf("Failed to listen on %s:%d: %v", bindIp, rpcPort, err)
	}
	storageORAMNodeMap := make(map[int]int)
	for _, s := range storages {
		storageORAMNodeMap[s.ID] = s.ORAMNodeID
	}
	log.Debug().Msgf("Created storage to ORAM Node map: %v", storageORAMNodeMap)

	shardnodeServer := newShardNodeServer(shardNodeServerID, replicaID, r, shardNodeFSM, oramNodeRPCClients, storageORAMNodeMap, parameters.TreeHeight, newBatchManager(time.Duration(parameters.BatchTimout)*time.Millisecond))
	log.Info().Msg("Starting batch sender.")
	go shardnodeServer.sendBatchesForever()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			shardnodeServer.shardNodeFSM.printStashSize()
		}
	}()

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	pb.RegisterShardNodeServer(grpcServer, shardnodeServer)
	log.Info().Msgf("Shard Node Server listening on %s:%d", bindIp, rpcPort)
	grpcServer.Serve(lis)
}
