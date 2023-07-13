package oramnode

import (
	"context"
	"fmt"
	"os"
	"testing"

	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/phayes/freeport"
	"google.golang.org/grpc"
)

type mockShardNodeClient struct {
	sendBlocksReply    func() (*shardnodepb.SendBlocksReply, error)
	ackSentBlocksReply func() (*shardnodepb.AckSentBlocksReply, error)
}

func (m *mockShardNodeClient) Read(ctx context.Context, in *shardnodepb.ReadRequest, opts ...grpc.CallOption) (*shardnodepb.ReadReply, error) {
	return nil, nil
}
func (m *mockShardNodeClient) Write(ctx context.Context, in *shardnodepb.WriteRequest, opts ...grpc.CallOption) (*shardnodepb.WriteReply, error) {
	return nil, nil
}
func (m *mockShardNodeClient) SendBlocks(ctx context.Context, in *shardnodepb.SendBlocksRequest, opts ...grpc.CallOption) (*shardnodepb.SendBlocksReply, error) {
	return m.sendBlocksReply()
}
func (m *mockShardNodeClient) AckSentBlocks(ctx context.Context, in *shardnodepb.AckSentBlocksRequest, opts ...grpc.CallOption) (*shardnodepb.AckSentBlocksReply, error) {
	return m.ackSentBlocksReply()
}
func (m *mockShardNodeClient) JoinRaftVoter(ctx context.Context, in *shardnodepb.JoinRaftVoterRequest, opts ...grpc.CallOption) (*shardnodepb.JoinRaftVoterReply, error) {
	return nil, nil
}

func cleanRaftDataDirectory(directoryPath string) {
	os.RemoveAll(directoryPath)
}

func getMockShardNodeClients() map[int]ReplicaRPCClientMap {
	return map[int]ReplicaRPCClientMap{
		0: map[int]ShardNodeRPCClient{
			0: {
				ClientAPI: &mockShardNodeClient{
					sendBlocksReply: func() (*shardnodepb.SendBlocksReply, error) {
						return &shardnodepb.SendBlocksReply{
							Blocks: []*shardnodepb.Block{
								{Block: "a", Value: "valA"},
								{Block: "b", Value: "valB"},
								{Block: "c", Value: "valC"},
								{Block: "d", Value: "valD"},
							},
						}, nil
					},
					ackSentBlocksReply: func() (*shardnodepb.AckSentBlocksReply, error) {
						return &shardnodepb.AckSentBlocksReply{Success: true}, nil
					},
				},
			},
			1: {
				ClientAPI: &mockShardNodeClient{
					sendBlocksReply: func() (*shardnodepb.SendBlocksReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
					ackSentBlocksReply: func() (*shardnodepb.AckSentBlocksReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
}

func getFailedMockShardNodeClients() map[int]ReplicaRPCClientMap {
	return map[int]ReplicaRPCClientMap{
		0: map[int]ShardNodeRPCClient{
			0: {
				ClientAPI: &mockShardNodeClient{
					sendBlocksReply: func() (*shardnodepb.SendBlocksReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
					ackSentBlocksReply: func() (*shardnodepb.AckSentBlocksReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
			1: {
				ClientAPI: &mockShardNodeClient{
					sendBlocksReply: func() (*shardnodepb.SendBlocksReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
					ackSentBlocksReply: func() (*shardnodepb.AckSentBlocksReply, error) {
						return nil, fmt.Errorf("not the leader")
					},
				},
			},
		},
	}
}

// TODO: duplicate code with the sharnode server_test
func startLeaderRaftNodeServer(t *testing.T, withFailedShardNodeClients bool) *oramNodeServer {
	cleanRaftDataDirectory("data-replicaid-0")

	fsm := newOramNodeFSM()
	raftPort, err := freeport.GetFreePort()
	if err != nil {
		t.Errorf("unable to get free port")
	}
	r, err := startRaftServer(true, 0, raftPort, fsm)
	if err != nil {
		t.Errorf("unable to start raft server")
	}
	<-r.LeaderCh() // wait to become the leader
	if withFailedShardNodeClients {
		return newOramNodeServer(0, 0, r, fsm, getFailedMockShardNodeClients(), storage.NewStorageHandler())
	} else {
		return newOramNodeServer(0, 0, r, fsm, getMockShardNodeClients(), storage.NewStorageHandler())
	}
}

func TestEvictCleansUpBeginEvictionAfterSuccessfulExecution(t *testing.T) {
	o := startLeaderRaftNodeServer(t, false)
	o.evict(0, 0)
	o.oramNodeFSM.mu.Lock()
	defer o.oramNodeFSM.mu.Unlock()

	if o.oramNodeFSM.unfinishedEviction != nil {
		t.Errorf("evict should remove unfinished eviction after successful eviction")
	}
}

func TestEvictKeepsBeginEvictionInFailureScenario(t *testing.T) {
	o := startLeaderRaftNodeServer(t, true)
	o.evict(0, 0)
	o.oramNodeFSM.mu.Lock()
	defer o.oramNodeFSM.mu.Unlock()
	if o.oramNodeFSM.unfinishedEviction == nil {
		t.Errorf("evict should add an unfinished eviction to FSM in failure scenarios")
	}
}
