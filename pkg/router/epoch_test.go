package router

import (
	"context"
	"fmt"
	"testing"
	"time"

	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"google.golang.org/grpc"
)

func TestAddRequestToCurrentEpochAddsRequestAndChannel(t *testing.T) {
	e := newEpochManager(make(map[int]ReplicaRPCClientMap), time.Second)
	e.currentEpoch = 12
	req := &request{ctx: context.Background(), requestId: "test_request_id", operationType: Read, block: "a", value: "value"}
	e.addRequestToCurrentEpoch(req)
	if len(e.requests[12]) != 1 || e.requests[12][0].requestId != "test_request_id" || e.requests[12][0].block != "a" || e.requests[12][0].value != "value" || e.requests[12][0].operationType != Read {
		t.Errorf("Expected request to be added to current epoch requests")
	}
	_, exists := e.reponseChans[12]["test_request_id"]
	if len(e.reponseChans[12]) != 1 || !exists {
		t.Errorf("Expected request to be added to the channel map")
	}
}

type whereToForwardTest struct {
	e                     *epochManager
	block                 string
	expectedShardNodeDest int
}

func createTestEpochManager(shardNodeRPCClientsCount int) (e *epochManager) {
	e = newEpochManager(make(map[int]ReplicaRPCClientMap), time.Second)
	for i := 0; i < shardNodeRPCClientsCount; i++ {
		e.shardNodeRPCClients[i] = make(ReplicaRPCClientMap)
	}
	return e
}

var testCases = []whereToForwardTest{
	{createTestEpochManager(1), "a", 0},
	{createTestEpochManager(1), "b", 0},
	{createTestEpochManager(2), "a", 0},
	{createTestEpochManager(2), "b", 1},
	{createTestEpochManager(2), "c", 0},
	{createTestEpochManager(2), "d", 1},
	{createTestEpochManager(2), "e", 0},
	{createTestEpochManager(3), "a", 1},
	{createTestEpochManager(3), "b", 1},
	{createTestEpochManager(3), "c", 2},
	{createTestEpochManager(3), "d", 1},
	{createTestEpochManager(3), "e", 2},
}

func TestWhereToForward(t *testing.T) {
	for _, test := range testCases {
		e := test.e
		output := e.whereToForward(test.block)
		if output != test.expectedShardNodeDest {
			t.Errorf("Block \"%s\" is getting forwarded to shard node number %d while it should go to shard node number %d",
				test.block, output, test.expectedShardNodeDest)
		}
	}
}

func TestGetShardnodeBatchesAddsEachRequestToCorrectBatch(t *testing.T) {
	e := createTestEpochManager(3)
	requests := []*request{
		{ctx: context.Background(), requestId: "1", operationType: Read, block: "a", value: "value"},
		{ctx: context.Background(), requestId: "2", operationType: Write, block: "b", value: "value"},
		{ctx: context.Background(), requestId: "3", operationType: Read, block: "c", value: "value"},
		{ctx: context.Background(), requestId: "4", operationType: Write, block: "d", value: "value"},
		{ctx: context.Background(), requestId: "5", operationType: Write, block: "e", value: "value"},
	}
	expectedBatchs := map[int]*shardnodepb.RequestBatch{
		0: {},
		1: {
			ReadRequests: []*shardnodepb.ReadRequest{
				{RequestId: "1", Block: "a"},
			},
			WriteRequests: []*shardnodepb.WriteRequest{
				{RequestId: "2", Block: "b", Value: "value"},
				{RequestId: "4", Block: "d", Value: "value"},
			},
		},
		2: {
			ReadRequests: []*shardnodepb.ReadRequest{
				{RequestId: "3", Block: "c"},
			},
			WriteRequests: []*shardnodepb.WriteRequest{
				{RequestId: "5", Block: "e", Value: "value"},
			},
		},
	}
	batches := e.getShardnodeBatches(requests)
	for shardNodeID, batch := range batches {
		if len(batch.ReadRequests) != len(expectedBatchs[shardNodeID].ReadRequests) || len(batch.WriteRequests) != len(expectedBatchs[shardNodeID].WriteRequests) {
			t.Errorf("Expected to see %d read requests and %d write requests for shard node %d", len(expectedBatchs[shardNodeID].ReadRequests), len(expectedBatchs[shardNodeID].WriteRequests), shardNodeID)
		}
		for i := 0; i < len(batch.ReadRequests); i++ {
			if batch.ReadRequests[i].RequestId != expectedBatchs[shardNodeID].ReadRequests[i].RequestId || batch.ReadRequests[i].Block != expectedBatchs[shardNodeID].ReadRequests[i].Block {
				t.Errorf("Expected to see read request %v at index %d for shard node %d", expectedBatchs[shardNodeID].ReadRequests[i], i, shardNodeID)
			}
		}
		for i := 0; i < len(batch.WriteRequests); i++ {
			if batch.WriteRequests[i].RequestId != expectedBatchs[shardNodeID].WriteRequests[i].RequestId || batch.WriteRequests[i].Block != expectedBatchs[shardNodeID].WriteRequests[i].Block || batch.WriteRequests[i].Value != expectedBatchs[shardNodeID].WriteRequests[i].Value {
				t.Errorf("Expected to see write request %v at index %d for shard node %d", expectedBatchs[shardNodeID].WriteRequests[i], i, shardNodeID)
			}
		}
	}
}

type mockShardNodeClient struct {
	batchReply func() (*shardnodepb.ReplyBatch, error)
}

func (m *mockShardNodeClient) BatchQuery(ctx context.Context, in *shardnodepb.RequestBatch, opts ...grpc.CallOption) (*shardnodepb.ReplyBatch, error) {
	return m.batchReply()
}
func (m *mockShardNodeClient) SendBlocks(ctx context.Context, in *shardnodepb.SendBlocksRequest, opts ...grpc.CallOption) (*shardnodepb.SendBlocksReply, error) {
	return nil, nil
}
func (m *mockShardNodeClient) AckSentBlocks(ctx context.Context, in *shardnodepb.AckSentBlocksRequest, opts ...grpc.CallOption) (*shardnodepb.AckSentBlocksReply, error) {
	return nil, nil
}
func (m *mockShardNodeClient) JoinRaftVoter(ctx context.Context, in *shardnodepb.JoinRaftVoterRequest, opts ...grpc.CallOption) (*shardnodepb.JoinRaftVoterReply, error) {
	return nil, nil
}

func getMockShardNodeClients() map[int]ReplicaRPCClientMap {
	return map[int]ReplicaRPCClientMap{
		0: map[int]ShardNodeRPCClient{
			0: {
				ClientAPI: &mockShardNodeClient{
					batchReply: func() (*shardnodepb.ReplyBatch, error) {
						return &shardnodepb.ReplyBatch{
							ReadReplies: []*shardnodepb.ReadReply{
								{RequestId: "a", Value: "123"},
							},
							WriteReplies: []*shardnodepb.WriteReply{
								{RequestId: "c", Success: true},
							},
						}, nil
					},
				},
			},
			1: {
				ClientAPI: &mockShardNodeClient{
					batchReply: func() (*shardnodepb.ReplyBatch, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
		1: map[int]ShardNodeRPCClient{
			0: {
				ClientAPI: &mockShardNodeClient{
					batchReply: func() (*shardnodepb.ReplyBatch, error) {
						return &shardnodepb.ReplyBatch{
							ReadReplies: []*shardnodepb.ReadReply{
								{RequestId: "b", Value: "123"},
							},
							WriteReplies: []*shardnodepb.WriteReply{
								{RequestId: "d", Success: true},
							},
						}, nil
					},
				},
			},
		},
	}
}

func TestSendEpochRequestsAndAnswerThemReturnsAllResponses(t *testing.T) {
	e := newEpochManager(getMockShardNodeClients(), time.Second)
	e.currentEpoch = 2
	request1 := &request{ctx: context.Background(), requestId: "a", operationType: Read, block: "a"}
	request2 := &request{ctx: context.Background(), requestId: "c", operationType: Write, block: "b", value: "123"}
	request3 := &request{ctx: context.Background(), requestId: "b", operationType: Read, block: "c"}
	request4 := &request{ctx: context.Background(), requestId: "d", operationType: Write, block: "d", value: "123"}
	e.requests[1] = []*request{
		request1, request2, request3, request4,
	}
	e.reponseChans[1] = make(map[string]chan any)
	chan1 := make(chan any)
	chan2 := make(chan any)
	chan3 := make(chan any)
	chan4 := make(chan any)
	e.reponseChans[1]["a"] = chan1
	e.reponseChans[1]["c"] = chan2
	e.reponseChans[1]["b"] = chan3
	e.reponseChans[1]["d"] = chan4

	go e.sendEpochRequestsAndAnswerThem(1, e.requests[1], e.reponseChans[1])
	timeout := time.After(5 * time.Second)
	responseCount := 0
	for {
		if responseCount == 4 {
			return
		}
		select {
		case <-timeout:
			t.Errorf("SendEpochRequestsAndAnswerThem should return all responses")
			return
		case <-chan1:
			responseCount++
		case <-chan2:
			responseCount++
		case <-chan3:
			responseCount++
		case <-chan4:
			responseCount++
		}
	}
}
