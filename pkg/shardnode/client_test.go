package shardnode

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"google.golang.org/grpc"
)

func TestGetRandomOramNodeReplicaMapReturnsRandomClientExistingInOramNodeMap(t *testing.T) {
	oramNodes := RPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {},
			1: {},
		},
		1: map[int]oramNodeRPCClient{
			0: {},
		},
	}
	random := oramNodes.getRandomOramNodeReplicaMap()
	for _, replicaMap := range oramNodes {
		if reflect.DeepEqual(random, replicaMap) {
			return
		}
	}
	t.Errorf("the random map does not exist")
}

type mockOramNodeClient struct {
	replyFunc func() (*oramnodepb.ReadPathReply, error)
}

func (c *mockOramNodeClient) ReadPath(ctx context.Context, in *oramnodepb.ReadPathRequest, opts ...grpc.CallOption) (*oramnodepb.ReadPathReply, error) {
	return c.replyFunc()
}

func (c *mockOramNodeClient) JoinRaftVoter(ctx context.Context, in *oramnodepb.JoinRaftVoterRequest, opts ...grpc.CallOption) (*oramnodepb.JoinRaftVoterReply, error) {
	return nil, nil
}

func TestReadPathFromAllOramNodeReplicasReturnsResponseFromLeader(t *testing.T) {
	oramNodeClients := map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func() (*oramnodepb.ReadPathReply, error) {
						return &oramnodepb.ReadPathReply{Responses: []*oramnodepb.BlockResponse{
							{Block: "a", Value: "response_from_leader"},
						}}, nil
					},
				},
			},
			1: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func() (*oramnodepb.ReadPathReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
	replicaMap := oramNodeClients[0]
	reply, err := replicaMap.readPathFromAllOramNodeReplicas(context.Background(), []blockRequest{{block: "a", path: 0}}, 0)
	if err != nil {
		t.Errorf("could not get the response that the leader returned. Error: %s", err)
	}
	if reply.Responses[0].Value != "response_from_leader" {
		t.Errorf("expected to get \"response_from_leader\" but got %s", reply.Responses[0].Value)
	}
}

func TestReadPathFromAllOramNodeReplicasTimeoutsIfNoResponseIsReceived(t *testing.T) {
	oramNodeClients := map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func() (*oramnodepb.ReadPathReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
			1: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func() (*oramnodepb.ReadPathReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
	replicaMap := oramNodeClients[0]
	_, err := replicaMap.readPathFromAllOramNodeReplicas(context.Background(), []blockRequest{{block: "a", path: 0}}, 0)
	if err == nil {
		t.Errorf("expected timeout error, but no error occurred.")
	}
}
