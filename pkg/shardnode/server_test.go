package shardnode

import (
	"context"
	"fmt"
	"testing"
	"time"

	oramnodepb "github.com/dsg-uwaterloo/treebeard/api/oramnode"
	shardnodepb "github.com/dsg-uwaterloo/treebeard/api/shardnode"
	"github.com/hashicorp/raft"
	"github.com/phayes/freeport"
)

func TestGetPathAndStorageBasedOnRequestWhenInitialRequestReturnsRealBlockAndPathAndStorage(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(0), nil, map[int]int{0: 0, 1: 1, 2: 2, 3: 3}, 5, newBatchManager(1))
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

func TestCreateResponseChannelForBatchAddsChannelToResponseChannel(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(0), nil, map[int]int{0: 0, 1: 1, 2: 2, 3: 3}, 5, newBatchManager(1))
	readRequests := []*shardnodepb.ReadRequest{
		{Block: "a", RequestId: "req1"},
		{Block: "b", RequestId: "req2"},
		{Block: "c", RequestId: "req3"},
	}
	writeRequests := []*shardnodepb.WriteRequest{
		{Block: "a", RequestId: "req1", Value: "val1"},
		{Block: "b", RequestId: "req2", Value: "val2"},
		{Block: "c", RequestId: "req3", Value: "val3"},
	}
	s.createResponseChannelForBatch(readRequests, writeRequests)
	for _, request := range readRequests {
		if _, exists := s.shardNodeFSM.responseChannel.Load(request.RequestId); !exists {
			t.Errorf("Expected a new channel for key %s but nothing found!", request.RequestId)
		}
	}
	for _, request := range writeRequests {
		if _, exists := s.shardNodeFSM.responseChannel.Load(request.RequestId); !exists {
			t.Errorf("Expected a new channel for key %s but nothing found!", request.RequestId)
		}
	}
}

func TestQueryBatchReturnsErrorForNonLeaderRaftPeer(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(0), nil, map[int]int{0: 0, 1: 1, 2: 2, 3: 3}, 5, newBatchManager(1))
	_, err := s.queryBatch(context.Background(), nil)
	if err == nil {
		t.Errorf("A non-leader raft peer should return error after call to query.")
	}
}

func getMockOramNodeClients() map[int]ReplicaRPCClientMap {
	return map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func(blocks []*oramnodepb.BlockRequest) (*oramnodepb.ReadPathReply, error) {
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
					replyFunc: func(blocks []*oramnodepb.BlockRequest) (*oramnodepb.ReadPathReply, error) {
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
					replyFunc: func([]*oramnodepb.BlockRequest) (*oramnodepb.ReadPathReply, error) {
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
					replyFunc: func([]*oramnodepb.BlockRequest) (*oramnodepb.ReadPathReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
}

func startLeaderRaftNodeServer(t *testing.T, batchSize int, withBatchReponses bool) *shardNodeServer {
	fsm := newShardNodeFSM(0)
	raftPort, err := freeport.GetFreePort()
	if err != nil {
		t.Errorf("unable to get free port")
	}
	r, err := startRaftServer(true, "localhost", "localhost", 0, raftPort, fsm)
	if err != nil {
		t.Errorf("unable to start raft server; %v", err)
	}
	<-r.LeaderCh() // wait to become the leader
	oramNodeClients := getMockOramNodeClients()
	if withBatchReponses {
		oramNodeClients = getMockOramNodeClientsWithBatchResponses()
	}
	s := newShardNodeServer(0, 0, r, fsm, oramNodeClients, map[int]int{0: 0}, 5, newBatchManager(2*time.Millisecond))
	go s.sendBatchesForever()
	return s
}

func TestSendCurrentBatchesSendsQueuesAfterBatchTimeout(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, getMockOramNodeClientsWithBatchResponses(), map[int]int{0: 0}, 5, newBatchManager(1*time.Millisecond))
	chA := make(chan string)
	s.batchManager.responseChannel["a"] = chA
	s.batchManager.storageQueues[1] = []blockRequest{{block: "a", path: 1}}
	chB := make(chan string)
	s.batchManager.responseChannel["b"] = chB
	s.batchManager.storageQueues[1] = append(s.batchManager.storageQueues[1], blockRequest{block: "b", path: 1})
	chC := make(chan string)
	s.batchManager.responseChannel["c"] = chC
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
		case <-chA:
			receivedResponsesCount++
		case <-chB:
			receivedResponsesCount++
		case <-chC:
			receivedResponsesCount++
		}
	}
}

func TestSendCurrentBatchesRemovesSentQueueAndResponseChannel(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, getMockOramNodeClients(), map[int]int{0: 0}, 5, newBatchManager(1))
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

func TestSendCurrentBatchesIgnoresEmptyQueues(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, getMockOramNodeClients(), map[int]int{0: 0}, 5, newBatchManager(1))
	chA := make(chan string)
	s.batchManager.responseChannel["a"] = chA
	s.batchManager.storageQueues[1] = []blockRequest{{block: "a", path: 1}}
	s.batchManager.storageQueues[2] = []blockRequest{}
	go s.sendCurrentBatches()
	select {
	case <-chA:
	case <-time.After(1 * time.Second):
		t.Errorf("SendCurrentBatches should ignore empty queues")
	}
}

func TestQueryBatchReturnsResponseRecievedFromOramNode(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)

	readRequests := []*shardnodepb.ReadRequest{
		{Block: "a", RequestId: "request1"},
		{Block: "b", RequestId: "request2"},
	}
	writeRequests := []*shardnodepb.WriteRequest{
		{Block: "c", RequestId: "request3", Value: "val1"},
	}

	response, err := s.queryBatch(context.Background(), &shardnodepb.RequestBatch{ReadRequests: readRequests, WriteRequests: writeRequests})
	if response == nil {
		t.Errorf("expected a response from the queryBatch call")
	}
	if err != nil {
		t.Errorf("expected no error in call to queryBatch")
	}
	expectedReadReplies := map[string]bool{
		"request1": true,
		"request2": true,
	}
	expectedWriteReplies := map[string]bool{
		"request3": true,
	}
	for _, readResponse := range response.ReadReplies {
		if _, exists := expectedReadReplies[readResponse.RequestId]; !exists {
			t.Errorf("expected the request id to be in the expectedReadReplies")
		}
		if readResponse.Value != "response_from_leader" {
			t.Errorf("expected the response to be \"response_from_leader\" but it is: %s", readResponse.Value)
		}
	}
	for _, writeResponse := range response.WriteReplies {
		if _, exists := expectedWriteReplies[writeResponse.RequestId]; !exists {
			t.Errorf("expected the request id to be in the expectedWriteReplies")
		}
		if !writeResponse.Success {
			t.Errorf("expected the write to be successful")
		}
	}
}

func TestQueryBatchReturnsResponseRecievedFromOramNodeWithBatching(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 3, true)

	responseChan := make(chan string)
	for _, el := range []string{"a", "b", "c"} {
		go func(block string) {
			requestBatch := &shardnodepb.RequestBatch{
				ReadRequests: []*shardnodepb.ReadRequest{
					{Block: block, RequestId: "request1"},
				},
				WriteRequests: []*shardnodepb.WriteRequest{},
			}
			response, err := s.queryBatch(context.Background(), requestBatch)
			if err == nil {
				responseChan <- response.ReadReplies[0].Value
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

func TestQueryBatchPrioritizesStashValueToOramNodeResponse(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	s.shardNodeFSM.stashMu.Lock()
	s.shardNodeFSM.stash["a"] = stashState{value: "stash_value", logicalTime: 0, waitingStatus: false}
	s.shardNodeFSM.stashMu.Unlock()
	requestBatch := &shardnodepb.RequestBatch{
		ReadRequests: []*shardnodepb.ReadRequest{
			{Block: "a", RequestId: "request1"},
		},
		WriteRequests: []*shardnodepb.WriteRequest{},
	}
	response, err := s.queryBatch(context.Background(), requestBatch)
	if response.ReadReplies[0].Value != "stash_value" {
		t.Errorf("expected the response to be \"stash_value\" but it is: %s", response)
	}
	if err != nil {
		t.Errorf("expected no error in call to query")
	}
}

func TestQueryBatchCleansTempValuesInFSMAfterExecution(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	requestBatch := &shardnodepb.RequestBatch{
		ReadRequests: []*shardnodepb.ReadRequest{
			{Block: "a", RequestId: "request1"},
			{Block: "b", RequestId: "request2"},
		},
		WriteRequests: []*shardnodepb.WriteRequest{
			{Block: "c", RequestId: "request3", Value: "val1"},
		},
	}
	s.queryBatch(context.Background(), requestBatch)
	for _, request := range []string{"request1", "request2", "request3"} {
		if _, exists := s.shardNodeFSM.pathMap[request]; exists {
			t.Errorf("query should remove the request from the pathMap after successful execution.")
		}
		if _, exists := s.shardNodeFSM.storageIDMap[request]; exists {
			t.Errorf("query should remove the request from the storageIDMap after successful execution.")
		}
		if _, exists := s.shardNodeFSM.requestLog[request]; exists {
			t.Errorf("query should remove the request from the requestLog after successful execution.")
		}
		if _, exists := s.shardNodeFSM.responseChannel.Load(request); exists {
			t.Errorf("query should remove the request from the responseChannel after successful execution.")
		}
	}
}

func TestQueryBatchAddsReadValueToStash(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	requestBatch := &shardnodepb.RequestBatch{
		ReadRequests: []*shardnodepb.ReadRequest{
			{Block: "a", RequestId: "request1"},
		},
		WriteRequests: []*shardnodepb.WriteRequest{},
	}
	s.queryBatch(context.Background(), requestBatch)
	s.shardNodeFSM.stashMu.Lock()
	defer s.shardNodeFSM.stashMu.Unlock()
	if s.shardNodeFSM.stash["a"].value != "response_from_leader" {
		t.Errorf("The response from the oramnode should be added to the stash")
	}
}

func TestQueryBatchAddsWriteValueToStash(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	requestBatch := &shardnodepb.RequestBatch{
		ReadRequests: []*shardnodepb.ReadRequest{},
		WriteRequests: []*shardnodepb.WriteRequest{
			{Block: "a", RequestId: "request1", Value: "val1"},
		},
	}
	s.queryBatch(context.Background(), requestBatch)
	s.shardNodeFSM.stashMu.Lock()
	defer s.shardNodeFSM.stashMu.Unlock()
	if s.shardNodeFSM.stash["a"].value != "val1" {
		t.Errorf("The write value should be added to the stash")
	}
}

func TestQueryBatchUpdatesPositionMap(t *testing.T) {
	s := startLeaderRaftNodeServer(t, 1, false)
	s.shardNodeFSM.positionMap["a"] = positionState{path: 13423432, storageID: 3223113}
	requestBatch := &shardnodepb.RequestBatch{
		ReadRequests: []*shardnodepb.ReadRequest{},
		WriteRequests: []*shardnodepb.WriteRequest{
			{Block: "a", RequestId: "request1", Value: "val1"},
		},
	}
	s.queryBatch(context.Background(), requestBatch)
	s.shardNodeFSM.positionMapMu.Lock()
	defer s.shardNodeFSM.positionMapMu.Unlock()
	if s.shardNodeFSM.positionMap["a"].path == 13423432 || s.shardNodeFSM.positionMap["a"].storageID == 3223113 {
		t.Errorf("position map should get updated after request")
	}
}

func TestGetBlocksForSendReturnsAtMostMaxBlocksFromTheStash(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(0), make(RPCClientMap), map[int]int{0: 0, 1: 1, 2: 2, 3: 3}, 5, newBatchManager(1))
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

	_, blocks := s.getBlocksForSend(4, 0)
	if len(blocks) != 4 {
		t.Errorf("expected 4 blocks but got: %d blocks", len(blocks))
	}
}

// func TestGetBlocksForSendReturnsOnlyBlocksForPathAndStorageID(t *testing.T) {
// 	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(0), make(RPCClientMap), map[int]int{0: 0, 1: 1, 2: 2, 3: 3}, 5, newBatchManager(1))
// 	s.shardNodeFSM.stash = map[string]stashState{
// 		"block1": {value: "block1", logicalTime: 0, waitingStatus: false},
// 		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
// 		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
// 	}
// 	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
// 	s.shardNodeFSM.positionMap["block2"] = positionState{path: 1, storageID: 2}
// 	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

// 	_, blocks := s.getBlocksForSend(4, 0)
// 	for _, block := range blocks {
// 		if block == "block2" {
// 			t.Errorf("getBlocks should only return blocks for the path and storageID")
// 		}
// 	}
// }

// func TestGetBlocksForSendDoesNotReturnsWaitingBlocks(t *testing.T) {
// 	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(0), make(RPCClientMap), map[int]int{0: 0, 1: 1, 2: 2, 3: 3}, 5, newBatchManager(1))
// 	s.shardNodeFSM.stash = map[string]stashState{
// 		"block1": {value: "block1", logicalTime: 0, waitingStatus: true},
// 		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
// 		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
// 	}
// 	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
// 	s.shardNodeFSM.positionMap["block2"] = positionState{path: 0, storageID: 0}
// 	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

// 	_, blocks := s.getBlocksForSend(4, []int{0}, 0)
// 	for _, block := range blocks {
// 		if block == "block1" {
// 			t.Errorf("getBlocks should only return blocks with the waitingStatus equal to false")
// 		}
// 	}
// }

// func TestSendBlocksReturnsStashBlocks(t *testing.T) {
// 	s := startLeaderRaftNodeServer(t, 1, false)
// 	s.shardNodeFSM.stash = map[string]stashState{
// 		"block1": {value: "block1", logicalTime: 0, waitingStatus: false},
// 		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
// 		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
// 	}
// 	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
// 	s.shardNodeFSM.positionMap["block2"] = positionState{path: 1, storageID: 0}
// 	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

// 	blocks, err := s.SendBlocks(context.Background(), &shardnodepb.SendBlocksRequest{MaxBlocks: 3, Paths: []int32{0, 1}, StorageId: 0})
// 	if err != nil {
// 		t.Errorf("Expected successful execution of SendBlocks")
// 	}
// 	if len(blocks.Blocks) != 3 {
// 		t.Errorf("Expected all values from the stash to return")
// 	}
// }

// func TestSendBlocksMarksSentBlocksAsWaitingAndZeroLogicalTime(t *testing.T) {
// 	s := startLeaderRaftNodeServer(t, 1, false)
// 	s.shardNodeFSM.stash = map[string]stashState{
// 		"block1": {value: "block1", logicalTime: 0, waitingStatus: false},
// 		"block2": {value: "block2", logicalTime: 0, waitingStatus: false},
// 		"block3": {value: "block3", logicalTime: 0, waitingStatus: false},
// 	}
// 	s.shardNodeFSM.positionMap["block1"] = positionState{path: 0, storageID: 0}
// 	s.shardNodeFSM.positionMap["block2"] = positionState{path: 0, storageID: 0}
// 	s.shardNodeFSM.positionMap["block3"] = positionState{path: 0, storageID: 0}

// 	blocks, _ := s.SendBlocks(context.Background(), &shardnodepb.SendBlocksRequest{MaxBlocks: 3, Paths: []int32{0}, StorageId: 0})
// 	s.shardNodeFSM.stashMu.Lock()
// 	for _, block := range blocks.Blocks {
// 		if s.shardNodeFSM.stash[block.Block].waitingStatus == false {
// 			t.Errorf("sent blocks should get marked as waiting")
// 		}
// 		if s.shardNodeFSM.stash[block.Block].logicalTime != 0 {
// 			t.Errorf("sent blocks should have logicalTime zero")
// 		}
// 	}
// 	s.shardNodeFSM.stashMu.Unlock()
// }

// func TestAckSentBlocksRemovesAckedBlocksFromStash(t *testing.T) {
// 	s := startLeaderRaftNodeServer(t, 1, false)
// 	s.shardNodeFSM.stash = map[string]stashState{
// 		"block1": {value: "block1", logicalTime: 0, waitingStatus: true},
// 		"block2": {value: "block2", logicalTime: 0, waitingStatus: true},
// 		"block3": {value: "block3", logicalTime: 0, waitingStatus: true},
// 	}
// 	s.AckSentBlocks(
// 		context.Background(),
// 		&shardnodepb.AckSentBlocksRequest{
// 			Acks: []*shardnodepb.Ack{
// 				{Block: "block1", IsAck: true},
// 				{Block: "block2", IsAck: true},
// 				{Block: "block3", IsAck: true},
// 			},
// 		},
// 	)
// 	time.Sleep(500 * time.Millisecond) // wait for handleLocalAcksNacksReplicationChanges goroutine to finish
// 	s.shardNodeFSM.stashMu.Lock()
// 	defer s.shardNodeFSM.stashMu.Unlock()
// 	if len(s.shardNodeFSM.stash) != 0 {
// 		t.Errorf("AckSentBlocks should remove all acked blocks from the stash but the stash is: %v", s.shardNodeFSM.stash)
// 	}
// }

// func TestAckSentBlocksKeepsNAckedBlocksInStashAndRemovesWaiting(t *testing.T) {
// 	s := startLeaderRaftNodeServer(t, 1, false)
// 	s.shardNodeFSM.stash = map[string]stashState{
// 		"block1": {value: "block1", logicalTime: 0, waitingStatus: true},
// 		"block2": {value: "block2", logicalTime: 0, waitingStatus: true},
// 		"block3": {value: "block3", logicalTime: 0, waitingStatus: true},
// 	}
// 	nackedBlocks := []*shardnodepb.Ack{
// 		{Block: "block1", IsAck: false},
// 		{Block: "block2", IsAck: false},
// 		{Block: "block3", IsAck: false},
// 	}
// 	s.AckSentBlocks(
// 		context.Background(),
// 		&shardnodepb.AckSentBlocksRequest{
// 			Acks: nackedBlocks,
// 		},
// 	)
// 	time.Sleep(500 * time.Millisecond) // wait for handleLocalAcksNacksReplicationChanges goroutine to finish
// 	s.shardNodeFSM.stashMu.Lock()
// 	defer s.shardNodeFSM.stashMu.Unlock()
// 	for _, block := range nackedBlocks {
// 		if s.shardNodeFSM.stash[block.Block].waitingStatus == true {
// 			t.Errorf("AckSentBlocks should remove waiting flag from nacked blocks")
// 		}
// 	}
// }
