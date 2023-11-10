package shardnode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dsg-uwaterloo/oblishard/api/oramnode"
	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/hashicorp/raft"
	"github.com/phayes/freeport"
	"google.golang.org/grpc/metadata"
)

func TestGetPathAndStorageBasedOnRequestWhenInitialRequestReturnsRealBlockAndPathAndStorage(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), nil, 4, 5, newBatchManager(1))
	s.shardNodeFSM.requestLog["block1"] = []string{"request1", "request2"}
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 23, storageID: 3}

	block, path, storageID := s.getWhatToSendBasedOnRequest(context.Background(), "block1", "request1", true)
	if block != "block1" {
		t.Errorf("Expected block to be \"block1\" but the value is: %s", block)
	}
	if path != 23 {
		t.Errorf("Expected path to be a real value from position map equal to 23 but the value is: %d", path)
	}
	if storageID != 3 {
		t.Errorf("Expected storageID to be a real value from position map equal to 3 but the value is: %d", storageID)
	}
}

func TestCreateResponseChannelForRequestIDAddsChannelToResponseChannel(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), nil, 4, 5, newBatchManager(1))
	s.createResponseChannelForRequestID("req1")
	if _, exists := s.shardNodeFSM.responseChannel.Load("req1"); !exists {
		t.Errorf("Expected a new channel for key req1 but nothing found!")
	}
}

func TestQueryReturnsErrorForNonLeaderRaftPeer(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), nil, 4, 5, newBatchManager(1))
	_, err := s.query(context.Background(), Read, "block", "")
	if err == nil {
		t.Errorf("A non-leader raft peer should return error after call to query.")
	}
}

func getMockOramNodeClients() map[int]ReplicaRPCClientMap {
	return map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func(blocks []*oramnode.BlockRequest) (*oramnodepb.ReadPathReply, error) {
						blocksToReturn := make([]*oramnodepb.BlockResponse, len(blocks))
						for i, block := range blocks {
							blocksToReturn[i] = &oramnodepb.BlockResponse{Block: block.Block, Value: "response_from_leader"}
						}
						return &oramnodepb.ReadPathReply{Responses: blocksToReturn}, nil
					},
				},
			},
			1: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func(blocks []*oramnode.BlockRequest) (*oramnodepb.ReadPathReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
}

func getMockOramNodeClientsWithBatchResponses() map[int]ReplicaRPCClientMap {
	return map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func([]*oramnode.BlockRequest) (*oramnodepb.ReadPathReply, error) {
						return &oramnodepb.ReadPathReply{Responses: []*oramnodepb.BlockResponse{
							{Block: "a", Value: "response_from_leader"},
							{Block: "b", Value: "response_from_leader"},
							{Block: "c", Value: "response_from_leader"},
						}}, nil
					},
				},
			},
			1: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func([]*oramnode.BlockRequest) (*oramnodepb.ReadPathReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
}

func startLeaderRaftNodeServer(t *testing.T, batchSize int, withBatchReponses bool) *shardNodeServer {
	fsm := newShardNodeFSM()
	raftPort, err := freeport.GetFreePort()
	if err != nil {
		t.Errorf("unable to get free port")
	}
	r, err := startRaftServer(true, "localhost", 0, raftPort, fsm)
	if err != nil {
		t.Errorf("unable to start raft server; %v", err)
	}
	fsm.raftNodeMu.Lock()
	fsm.raftNode = r
	fsm.raftNodeMu.Unlock()
	<-r.LeaderCh() // wait to become the leader
	oramNodeClients := getMockOramNodeClients()
	if withBatchReponses {
		oramNodeClients = getMockOramNodeClientsWithBatchResponses()
	}
	s := newShardNodeServer(0, 0, r, fsm, oramNodeClients, 4, 5, newBatchManager(2*time.Millisecond))
	go s.sendBatchesForever()
	return s
}

func TestSendCurrentBatchesSendsQueuesExceedingBatchSizeRequests(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, getMockOramNodeClientsWithBatchResponses(), 4, 5, newBatchManager(3))
	s.batchManager.responseChannel["a"] = make(chan string)
	s.batchManager.storageQueues[1] = []blockRequest{{block: "a", path: 1}}
	s.batchManager.responseChannel["b"] = make(chan string)
	s.batchManager.storageQueues[1] = append(s.batchManager.storageQueues[1], blockRequest{block: "b", path: 1})
	s.batchManager.responseChannel["c"] = make(chan string)
	s.batchManager.storageQueues[1] = append(s.batchManager.storageQueues[1], blockRequest{block: "c", path: 1})
	go s.sendCurrentBatches()
	timout := time.After(3 * time.Second)
	receivedResponsesCount := 0
	for {
		if receivedResponsesCount == 3 {
			break
		}
		select {
		case <-timout:
			t.Errorf("the batches were not sent")
			return
		case <-s.batchManager.responseChannel["a"]:
			receivedResponsesCount++
		case <-s.batchManager.responseChannel["b"]:
			receivedResponsesCount++
		case <-s.batchManager.responseChannel["c"]:
			receivedResponsesCount++
		}
	}
}

func TestSendCurrentBatchesOnlySendsBatchSizeRequestsAtATime(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, getMockOramNodeClientsWithBatchResponses(), 4, 5, newBatchManager(2))
	s.batchManager.responseChannel["a"] = make(chan string)
	s.batchManager.storageQueues[1] = []blockRequest{{block: "a", path: 1}}
	s.batchManager.responseChannel["b"] = make(chan string)
	s.batchManager.storageQueues[1] = append(s.batchManager.storageQueues[1], blockRequest{block: "b", path: 1})
	s.batchManager.responseChannel["c"] = make(chan string)
	s.batchManager.storageQueues[1] = append(s.batchManager.storageQueues[1], blockRequest{block: "c", path: 1})
	go s.sendCurrentBatches()
	timout := time.After(3 * time.Second)
	receivedResponsesCount := 0
	for {
		if receivedResponsesCount == 2 {
			break
		}
		select {
		case <-timout:
			t.Errorf("the batches were not sent")
			return
		case <-s.batchManager.responseChannel["a"]:
			receivedResponsesCount++
		case <-s.batchManager.responseChannel["b"]:
			receivedResponsesCount++
		}
	}
	if len(s.batchManager.storageQueues[1]) != 1 || s.batchManager.storageQueues[1][0].block != "c" {
		t.Errorf("the remaining request should be c")
	}
}

func TestSendCurrentBatchesRemovesSentQueueAndResponseChannel(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, getMockOramNodeClients(), 4, 5, newBatchManager(1))
	s.batchManager.responseChannel["a"] = make(chan string)
	s.batchManager.storageQueues[1] = []blockRequest{{block: "a", path: 1}}
	go s.sendCurrentBatches()
	<-s.batchManager.responseChannel["a"]
	if len(s.batchManager.storageQueues[1]) != 0 {
		t.Errorf("SendCurrentBatches should remove queue after sending it")
	}
	if _, exists := s.batchManager.responseChannel["a"]; exists {
		t.Errorf("SendCurrentBatches should remove block from response channel after sending it")
	}
}

func TestQueryReturnsResponseRecievedFromOramNode(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	response, err := s.query(ctx, Read, "a", "")
	if response != "response_from_leader" {
		t.Errorf("expected the response to be \"response_from_leader\" but it is: %s", response)
	}
	if err != nil {
		t.Errorf("expected no error in call to query")
	}
}

func TestQueryReturnsResponseRecievedFromOramNodeWithBatching(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 3, true)

	responseChan := make(chan string)
	for _, el := range []string{"a", "b", "c"} {
		go func(block string) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", fmt.Sprintf("request:%s", block)))
			response, err := s.query(ctx, Read, block, "")
			if err == nil {
				responseChan <- response
			}
		}(el)
	}
	timeout := time.After(3 * time.Second)
	for i := 0; i < 3; i++ {
		var response string
		select {
		case <-timeout:
			t.Errorf("expected response for all blocks in the batch")
			return
		case response = <-responseChan:
		}

		if response != "response_from_leader" {
			t.Errorf("expected the response to be \"response_from_leader\" but it is: %s", response)
		}
	}
}

func TestQueryPrioritizesStashValueToOramNodeResponse(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.shardNodeFSM.stashMu.Lock()
	s.shardNodeFSM.stash["a"] = stashState{value: "stash_value", logicalTime: 0, waitingStatus: false}
	s.shardNodeFSM.stashMu.Unlock()
	response, err := s.query(ctx, Read, "a", "")
	if response != "stash_value" {
		t.Errorf("expected the response to be \"stash_value\" but it is: %s", response)
	}
	if err != nil {
		t.Errorf("expected no error in call to query")
	}
}

func TestQueryReturnsSentValueForWriteRequests(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	response, err := s.query(ctx, Write, "a", "val")
	if response != "val" {
		t.Errorf("expected the response to be \"val\" but it is: %s", response)
	}
	if err != nil {
		t.Errorf("expected no error in call to query")
	}
}

func TestQueryCleansTempValuesInFSMAfterExecution(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.query(ctx, Write, "a", "val")
	s.shardNodeFSM.pathMapMu.Lock()
	s.shardNodeFSM.storageIDMapMu.Lock()
	s.shardNodeFSM.responseMapMu.Lock()
	s.shardNodeFSM.requestLogMu.Lock()
	defer func() {
		s.shardNodeFSM.pathMapMu.Unlock()
		s.shardNodeFSM.storageIDMapMu.Unlock()
		s.shardNodeFSM.responseMapMu.Unlock()
		s.shardNodeFSM.requestLogMu.Unlock()
	}()
	if _, exists := s.shardNodeFSM.pathMap["request1"]; exists {
		t.Errorf("query should remove the request from the pathMap after successful execution.")
	}
	if _, exists := s.shardNodeFSM.storageIDMap["request1"]; exists {
		t.Errorf("query should remove the request from the storageIDMap after successful execution.")
	}
	if _, exists := s.shardNodeFSM.responseMap["request1"]; exists {
		t.Errorf("query should remove the request from the responseMap after successful execution.")
	}
	if _, exists := s.shardNodeFSM.requestLog["request1"]; exists {
		t.Errorf("query should remove the request from the requestLog after successful execution.")
	}
	if _, exists := s.shardNodeFSM.responseChannel.Load("request1"); exists {
		t.Errorf("query should remove the request from the responseChannel after successful execution.")
	}
}

func TestQueryAddsReadValueToStash(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.query(ctx, Read, "a", "")
	s.shardNodeFSM.stashMu.Lock()
	defer s.shardNodeFSM.stashMu.Unlock()
	if s.shardNodeFSM.stash["a"].value != "response_from_leader" {
		t.Errorf("The response from the oramnode should be added to the stash")
	}
}

func TestQueryAddsWriteValueToStash(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.query(ctx, Write, "a", "valW")
	s.shardNodeFSM.stashMu.Lock()
	defer s.shardNodeFSM.stashMu.Unlock()
	if s.shardNodeFSM.stash["a"].value != "valW" {
		t.Errorf("The write value should be added to the stash")
	}
}

func TestQueryUpdatesPositionMap(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.shardNodeFSM.positionMap["a"] = positionState{path: 13423432, storageID: 3223113}
	s.query(ctx, Write, "a", "valW")
	s.shardNodeFSM.positionMapMu.Lock()
	defer s.shardNodeFSM.positionMapMu.Unlock()
	if s.shardNodeFSM.positionMap["a"].path == 13423432 || s.shardNodeFSM.positionMap["a"].storageID == 3223113 {
		t.Errorf("position map should get updated after request")
	}
}

func TestQueryReturnsResponseToAllWaitingRequests(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	responseChannel := make(chan string)
	for i := 0; i < 3; i++ {
		go func(idx int) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", fmt.Sprintf("request%d", idx)))
			response, _ := s.query(ctx, Read, "a", "")
			responseChannel <- response
		}(i)
	}
	responseCount := 0
	timout := time.After(10 * time.Second)
	for {
		if responseCount == 2 {
			break
		}
		select {
		case <-responseChannel:
			responseCount++
		case <-timout:
			t.Errorf("timeout before receiving all responses")
		}
	}
}

func TestGetBlocksForSendReturnsAtMostMaxBlocksFromTheStash(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), make(RPCClientMap), 4, 5, newBatchManager(1))
	s.shardNodeFSM.stash = map[string]stashState{
		"block1": {value: "block1", logicalTime: 0, waitingStatus: false},
		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
		"block4": {value: "block4", logicalTime: 0, waitingStatus: false},
		"block5": {value: "block5", logicalTime: 0, waitingStatus: false},
		"block6": {value: "block6", logicalTime: 0, waitingStatus: false},
	}
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block2"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block4"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block5"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block6"] = positionState{path: 0, storageID: 0}

	_, blocks := s.getBlocksForSend(4, []int{0}, 0)
	if len(blocks) != 4 {
		t.Errorf("expected 4 blocks but got: %d blocks", len(blocks))
	}
}

func TestGetBlocksForSendReturnsOnlyBlocksForPathAndStorageID(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), make(RPCClientMap), 4, 5, newBatchManager(1))
	s.shardNodeFSM.stash = map[string]stashState{
		"block1": {value: "block1", logicalTime: 0, waitingStatus: false},
		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
	}
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block2"] = positionState{path: 1, storageID: 2}
	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

	_, blocks := s.getBlocksForSend(4, []int{0}, 0)
	for _, block := range blocks {
		if block == "block2" {
			t.Errorf("getBlocks should only return blocks for the path and storageID")
		}
	}
}

func TestGetBlocksForSendDoesNotReturnsWaitingBlocks(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), make(RPCClientMap), 4, 5, newBatchManager(1))
	s.shardNodeFSM.stash = map[string]stashState{
		"block1": {value: "block1", logicalTime: 0, waitingStatus: true},
		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
	}
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block2"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

	_, blocks := s.getBlocksForSend(4, []int{0}, 0)
	for _, block := range blocks {
		if block == "block1" {
			t.Errorf("getBlocks should only return blocks with the waitingStatus equal to false")
		}
	}
}

func TestSendBlocksReturnsStashBlocks(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	s.shardNodeFSM.stash = map[string]stashState{
		"block1": {value: "block1", logicalTime: 0, waitingStatus: false},
		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
	}
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block2"] = positionState{path: 1, storageID: 0}
	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

	blocks, err := s.SendBlocks(context.Background(), &shardnodepb.SendBlocksRequest{MaxBlocks: 3, Paths: []int32{0, 1}, StorageId: 0})
	if err != nil {
		t.Errorf("Expected successful execution of SendBlocks")
	}
	if len(blocks.Blocks) != 3 {
		t.Errorf("Expected all values from the stash to return")
	}
}

func TestSendBlocksMarksSentBlocksAsWaitingAndZeroLogicalTime(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	s.shardNodeFSM.stash = map[string]stashState{
		"block1": {value: "block1", logicalTime: 0, waitingStatus: false},
		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
	}
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block2"] = positionState{path: 0, storageID: 0}
	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

	blocks, _ := s.SendBlocks(context.Background(), &shardnodepb.SendBlocksRequest{MaxBlocks: 3, Paths: []int32{0}, StorageId: 0})
	s.shardNodeFSM.stashMu.Lock()
	for _, block := range blocks.Blocks {
		if s.shardNodeFSM.stash[block.Block].waitingStatus == false {
			t.Errorf("sent blocks should get marked as waiting")
		}
		if s.shardNodeFSM.stash[block.Block].logicalTime != 0 {
			t.Errorf("sent blocks should have logicalTime zero")
		}
	}
	s.shardNodeFSM.stashMu.Unlock()
}

func TestAckSentBlocksRemovesAckedBlocksFromStash(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	s.shardNodeFSM.stash = map[string]stashState{
		"block1": {value: "block1", logicalTime: 0, waitingStatus: true},
		"block2": {value: "block2", logicalTime: 0, waitingStatus: true},
		"block3": {value: "block3", logicalTime: 0, waitingStatus: true},
	}
	s.AckSentBlocks(
		context.Background(),
		&shardnodepb.AckSentBlocksRequest{
			Acks: []*shardnodepb.Ack{
				{Block: "block1", IsAck: true},
				{Block: "block2", IsAck: true},
				{Block: "block3", IsAck: true},
			},
		},
	)
	time.Sleep(500 * time.Millisecond) // wait for handleLocalAcksNacksReplicationChanges goroutine to finish
	s.shardNodeFSM.stashMu.Lock()
	defer s.shardNodeFSM.stashMu.Unlock()
	if len(s.shardNodeFSM.stash) != 0 {
		t.Errorf("AckSentBlocks should remove all acked blocks from the stash but the stash is: %v", s.shardNodeFSM.stash)
	}
}

func TestAckSentBlocksKeepsNAckedBlocksInStashAndRemovesWaiting(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	s.shardNodeFSM.stash = map[string]stashState{
		"block1": {value: "block1", logicalTime: 0, waitingStatus: true},
		"block2": {value: "block2", logicalTime: 0, waitingStatus: true},
		"block3": {value: "block3", logicalTime: 0, waitingStatus: true},
	}
	nackedBlocks := []*shardnodepb.Ack{
		{Block: "block1", IsAck: false},
		{Block: "block2", IsAck: false},
		{Block: "block3", IsAck: false},
	}
	s.AckSentBlocks(
		context.Background(),
		&shardnodepb.AckSentBlocksRequest{
			Acks: nackedBlocks,
		},
	)
	time.Sleep(500 * time.Millisecond) // wait for handleLocalAcksNacksReplicationChanges goroutine to finish
	s.shardNodeFSM.stashMu.Lock()
	defer s.shardNodeFSM.stashMu.Unlock()
	for _, block := range nackedBlocks {
		if s.shardNodeFSM.stash[block.Block].waitingStatus == true {
			t.Errorf("AckSentBlocks should remove waiting flag from nacked blocks")
		}
	}
}
