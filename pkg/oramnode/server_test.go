package oramnode

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/dsg-uwaterloo/oblishard/api/oramnode"
	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	strg "github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/phayes/freeport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

type mockStorageHandler struct {
	levelCount      int
	maxAccessCount  int
	latestReadBlock int
	writeBucketFunc func(level int, path int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error)
}

func newMockStorageHandler(levelCount int, maxAccessCount int) *mockStorageHandler {
	return &mockStorageHandler{
		levelCount:      levelCount,
		maxAccessCount:  maxAccessCount,
		latestReadBlock: 0,
		writeBucketFunc: func(level, path, storageID int, ReadBucketBlocks, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error) {
			writtenBlocks = make(map[string]string)
			for block, value := range ReadBucketBlocks {
				writtenBlocks[block] = value
			}
			for block, value := range shardNodeBlocks {
				writtenBlocks[block] = value
			}
			return writtenBlocks, nil
		},
	}
}

func (m *mockStorageHandler) withCustomWriteFunc(customeWriteFunc func(level int, path int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error)) *mockStorageHandler {
	m.writeBucketFunc = customeWriteFunc
	return m
}

func (m *mockStorageHandler) GetLevelCount() int {
	return m.levelCount
}

func (m *mockStorageHandler) GetMaxAccessCount() int {
	return m.maxAccessCount
}

func (m *mockStorageHandler) GetRandomPathAndStorageID() (path int, storageID int) {
	return 0, 0
}

func (m *mockStorageHandler) GetBlockOffset(level int, path int, storageID int, block string) (offset int, err error) {
	return level, nil
}
func (m *mockStorageHandler) GetAccessCount(level int, path int, storageID int) (count int, err error) {
	return 0, nil
}

func (m *mockStorageHandler) ReadBucket(level int, path int, storageID int) (blocks map[string]string, err error) {
	blocks = make(map[string]string)
	for i := m.latestReadBlock; i < m.latestReadBlock+4; i++ {
		blocks[strconv.Itoa(i)] = "value"
	}
	m.latestReadBlock += 4
	return blocks, nil
}

func (m *mockStorageHandler) WriteBucket(level int, path int, storageID int, readBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error) {
	return m.writeBucketFunc(level, path, storageID, readBucketBlocks, shardNodeBlocks, isAtomic)
}

func (m *mockStorageHandler) ReadBlock(level int, path int, storageID int, offset int) (value string, err error) {
	return "", nil
}

func startLeaderRaftNodeServer(t *testing.T) *oramNodeServer {
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
	return newOramNodeServer(0, 0, r, fsm, getMockShardNodeClients(), strg.NewStorageHandler())
}

func (o *oramNodeServer) withFailedShardNodeClients() *oramNodeServer {
	o.shardNodeRPCClients = getFailedMockShardNodeClients()
	return o
}

func (o *oramNodeServer) withMockStorageHandler(storageHandler *mockStorageHandler) *oramNodeServer {
	o.storageHandler = storageHandler
	return o
}

func TestReadBucketAllLevelsReturnsAllTreeBlocks(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(12, 4))
	blocks, err := o.readBucketAllLevels(0, 1)
	if err != nil {
		t.Errorf("Expected successful execution of readBucketAllLevels")
	}

	for level := 0; level < 12; level++ {
		fmt.Println(level)
		for i := 4 * level; i < 4*level+4; i++ {
			fmt.Println(i)
			if _, exists := blocks[level]; !exists {
				t.Errorf("all tree values should be in the blocksFromReadBucket")
			}
		}
	}
}

func TestReadBlocksFromShardNodeReturnsAllShardNodeBlocks(t *testing.T) {
	o := startLeaderRaftNodeServer(t)
	blocks, err := o.readBlocksFromShardNode(0, 1, o.shardNodeRPCClients[0])
	if err != nil {
		t.Errorf("expected Successful execution of readBlocksFromShardNode")
	}
	expectedBlocks := []string{"a", "b", "c", "d"}
	for _, block := range expectedBlocks {
		if _, exists := blocks[block]; !exists {
			t.Errorf("expected value %s to be in the received blocks", block)
		}
	}
}

func TestWriteBackBlocksToAllLevelsPushesReceivedBlocksToTree(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(3, 4))
	receivedBlocksIsWritten, err := o.writeBackBlocksToAllLevels(0,
		1,
		map[int]map[string]string{
			0: {
				"1": "val1",
			},
			1: {
				"2": "val1",
			},
			2: {
				"3": "val1",
			},
		},
		map[string]string{
			"a": "valA",
			"b": "valB",
			"c": "valC",
		},
	)
	if err != nil {
		t.Errorf("expected successful execution of writeBackBlocksToAllLevels")
	}
	for _, block := range []string{"a", "b", "c"} {
		if receivedBlocksIsWritten[block] == false {
			t.Errorf("expected is written to be true for block %s", block)
		}
	}
}

func TestWriteBackBlocksToAllLevelsReturnsFalseForNotPushedReceivedBlocks(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(3, 4).withCustomWriteFunc(
		func(level, path, storageID int, ReadBucketBlocks, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error) {
			writtenBlocks = make(map[string]string)
			for block, val := range shardNodeBlocks {
				if block != "a" {
					writtenBlocks[block] = val
				}
			}
			return writtenBlocks, nil
		},
	))
	receivedBlocksIsWritten, err := o.writeBackBlocksToAllLevels(0,
		1,
		map[int]map[string]string{
			0: {
				"1": "val1",
			},
			1: {
				"2": "val1",
			},
			2: {
				"3": "val1",
			},
		},
		map[string]string{
			"a": "valA",
			"b": "valB",
			"c": "valC",
		},
	)
	if err != nil {
		t.Errorf("expected successful execution of writeBackBlocksToAllLevels")
	}
	if receivedBlocksIsWritten["a"] == true {
		t.Errorf("IsWritten should be false for a since it's not written to the tree")
	}
}

func TestEvictCleansUpBeginEvictionAfterSuccessfulExecution(t *testing.T) {
	o := startLeaderRaftNodeServer(t)
	o.evict(0, 0)
	o.oramNodeFSM.mu.Lock()
	defer o.oramNodeFSM.mu.Unlock()

	if o.oramNodeFSM.unfinishedEviction != nil {
		t.Errorf("evict should remove unfinished eviction after successful eviction")
	}
}

func TestEvictKeepsBeginEvictionInFailureScenario(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withFailedShardNodeClients()
	o.evict(0, 0)
	o.oramNodeFSM.mu.Lock()
	defer o.oramNodeFSM.mu.Unlock()
	if o.oramNodeFSM.unfinishedEviction == nil {
		t.Errorf("evict should add an unfinished eviction to FSM in failure scenarios")
	}
}

func TestEvictResetsReadPathCounter(t *testing.T) {
	o := startLeaderRaftNodeServer(t)
	o.evict(0, 0)
	o.readPathCounterMu.Lock()
	defer o.readPathCounterMu.Unlock()
	if o.readPathCounter != 0 {
		t.Errorf("Evict should reset readPathCounter after successful execution")
	}
}

func TestReadPathRemovesOffsetListAfterSuccessfulExecution(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(4, 4))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	o.ReadPath(ctx, &oramnode.ReadPathRequest{Block: "a", Path: 1, StorageId: 2})
	o.oramNodeFSM.mu.Lock()
	defer o.oramNodeFSM.mu.Unlock()
	if offsetList, exists := o.oramNodeFSM.offsetListMap["request1"]; exists {
		t.Errorf("expected that ReadPath removes offset list after successful execution, but it's equal to %v", offsetList)
	}
}

func TestReadPathIncrementsReadPathCounter(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(4, 4))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	o.ReadPath(ctx, &oramnode.ReadPathRequest{Block: "a", Path: 1, StorageId: 2})
	o.oramNodeFSM.mu.Lock()
	defer o.oramNodeFSM.mu.Unlock()
	if o.readPathCounter != 1 {
		t.Errorf("ReadPath should increment readPathCounter")
	}
}
