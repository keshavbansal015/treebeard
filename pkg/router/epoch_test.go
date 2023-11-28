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
	req := &request{ctx: context.Background(), operationType: Read, block: "a", value: "value"}
	e.addRequestToCurrentEpoch(req)
	if len(e.requests[12]) != 1 || e.requests[12][0].block != "a" || e.requests[12][0].value != "value" || e.requests[12][0].operationType != Read {
		t.Errorf("Expected request to be added to current epoch requests")
	}
	_, exists := e.reponseChans[12][req]
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

type mockShardNodeClient struct {
	readReply  func() (*shardnodepb.ReadReply, error)
	writeReply func() (*shardnodepb.WriteReply, error)
}

func (m *mockShardNodeClient) Read(ctx context.Context, in *shardnodepb.ReadRequest, opts ...grpc.CallOption) (*shardnodepb.ReadReply, error) {
	return m.readReply()
}
func (m *mockShardNodeClient) Write(ctx context.Context, in *shardnodepb.WriteRequest, opts ...grpc.CallOption) (*shardnodepb.WriteReply, error) {
	return m.writeReply()
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
					readReply: func() (*shardnodepb.ReadReply, error) {
						return &shardnodepb.ReadReply{Value: "valA"}, nil
					},
					writeReply: func() (*shardnodepb.WriteReply, error) {
						return &shardnodepb.WriteReply{Success: true}, nil
					},
				},
			},
			1: {
				ClientAPI: &mockShardNodeClient{
					readReply: func() (*shardnodepb.ReadReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
					writeReply: func() (*shardnodepb.WriteReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
}

func TestSendEpochRequestsAndAnswerThemReturnsAllResponses(t *testing.T) {
	e := newEpochManager(getMockShardNodeClients(), time.Second)
	e.currentEpoch = 2
	request1 := &request{ctx: context.Background(), operationType: Write, block: "a", value: "123"}
	request2 := &request{ctx: context.Background(), operationType: Read, block: "b"}
	e.requests[1] = []*request{
		request1, request2,
	}
	e.reponseChans[1] = make(map[*request]chan any)
	chan1 := make(chan any)
	chan2 := make(chan any)
	e.reponseChans[1][request1] = chan1
	e.reponseChans[1][request2] = chan2

	go e.sendEpochRequestsAndAnswerThem(1, e.requests[1], e.reponseChans[1])
	timeout := time.After(5 * time.Second)
	responseCount := 0
	for {
		if responseCount == 2 {
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
		}
	}
}
