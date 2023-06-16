package shardnode

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

func TestGetRandomOramNodeReplicaMapReturnsRandomClientExistingInOramNodeMap(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, make(map[int]ReplicaRPCClientMap))
	s.oramNodeClients = map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {},
			1: {},
		},
		1: map[int]oramNodeRPCClient{
			0: {},
		},
	}
	random := s.getRandomOramNodeReplicaMap()
	for _, replicaMap := range s.oramNodeClients {
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
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, make(map[int]ReplicaRPCClientMap))
	s.oramNodeClients = map[int]ReplicaRPCClientMap{
		0: map[int]oramNodeRPCClient{
			0: {
				ClientAPI: &mockOramNodeClient{
					replyFunc: func() (*oramnodepb.ReadPathReply, error) {
						return &oramnodepb.ReadPathReply{Value: "response_from_leader"}, nil
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
	reply, err := s.readPathFromAllOramNodeReplicas(context.Background(), s.oramNodeClients[0], "", 0, 0, true)
	if err != nil {
		t.Errorf("could not get the response that the leader returned. Error: %s", err)
	}
	if reply.Value != "response_from_leader" {
		t.Errorf("expected to get \"response_from_leader\" but got %s", reply.Value)
	}
}

func TestReadPathFromAllOramNodeReplicasTimeoutsIfNoResponseIsReceived(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, &shardNodeFSM{}, make(map[int]ReplicaRPCClientMap))
	s.oramNodeClients = map[int]ReplicaRPCClientMap{
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
	_, err := s.readPathFromAllOramNodeReplicas(context.Background(), s.oramNodeClients[0], "", 0, 0, true)
	if err == nil {
		t.Errorf("expected timeout error, but no error occurred.")
	}
}
