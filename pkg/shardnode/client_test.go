package shardnode

import (
	"context"
	"fmt"
	"testing"

	"github.com/dsg-uwaterloo/oblishard/api/oramnode"
	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"google.golang.org/grpc"
)

type mockOramNodeClient struct {
	replyFunc func([]*oramnode.BlockRequest) (*oramnodepb.ReadPathReply, error)
}

func (c *mockOramNodeClient) ReadPath(ctx context.Context, in *oramnodepb.ReadPathRequest, opts ...grpc.CallOption) (*oramnodepb.ReadPathReply, error) {
	return c.replyFunc(in.Requests)
}

func (c *mockOramNodeClient) JoinRaftVoter(ctx context.Context, in *oramnodepb.JoinRaftVoterRequest, opts ...grpc.CallOption) (*oramnodepb.JoinRaftVoterReply, error) {
	return nil, nil
}

func TestReadPathFromAllOramNodeReplicasReturnsResponseFromLeader(t *testing.T) {
	oramNodeClients := map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func([]*oramnode.BlockRequest) (*oramnodepb.ReadPathReply, error) {
						return &oramnodepb.ReadPathReply{Responses: []*oramnodepb.BlockResponse{
							{Block: "a", Value: "response_from_leader"},
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
					replyFunc: func([]*oramnode.BlockRequest) (*oramnodepb.ReadPathReply, error) {
						return nil, fmt.Errorf("not the leader")
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
	replicaMap := oramNodeClients[0]
	_, err := replicaMap.readPathFromAllOramNodeReplicas(context.Background(), []blockRequest{{block: "a", path: 0}}, 0)
	if err == nil {
		t.Errorf("expected timeout error, but no error occurred.")
	}
}
