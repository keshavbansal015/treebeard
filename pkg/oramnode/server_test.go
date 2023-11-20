package oramnode

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/dsg-uwaterloo/oblishard/api/oramnode"
	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	strg "github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/hashicorp/raft"
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
	levelCount               int
	maxAccessCount           int
	latestReadBlock          int
	writeBucketFunc          func(bucketID int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error)
	readBlockAccessedOffsets []int
}

func newMockStorageHandler(levelCount int, maxAccessCount int) *mockStorageHandler {
	return &mockStorageHandler{
		levelCount:      levelCount,
		maxAccessCount:  maxAccessCount,
		latestReadBlock: 0,
		writeBucketFunc: func(_ int, _ int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error) {
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

func (m *mockStorageHandler) withCustomWriteFunc(customeWriteFunc func(bucketID int, storageID int, ReadBucketBlocks map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error)) *mockStorageHandler {
	m.writeBucketFunc = customeWriteFunc
	return m
}

func (m *mockStorageHandler) GetMaxAccessCount() int {
	return m.maxAccessCount
}

func (m *mockStorageHandler) LockStorage(storageID int) {
}

func (m *mockStorageHandler) UnlockStorage(storageID int) {
}

func (m *mockStorageHandler) GetRandomPathAndStorageID(context.Context) (path int, storageID int) {
	return 0, 0
}

func (m *mockStorageHandler) GetBlockOffset(bucketID int, storageID int, blocks []string) (offset int, isReal bool, blockFound string, err error) {
	return 4, true, "a", nil
}
func (m *mockStorageHandler) GetAccessCount(bucketID int, storageID int) (count int, err error) {
	return 0, nil
}

// It returns four blocks with block id m.latestReadBlock to m.latestReadBlock+3
func (m *mockStorageHandler) ReadBucket(bucketID int, storageID int) (blocks map[string]string, err error) {
	blocks = make(map[string]string)
	for i := m.latestReadBlock; i < m.latestReadBlock+4; i++ {
		blocks[strconv.Itoa(i)] = "value"
	}
	m.latestReadBlock += 4
	return blocks, nil
}

func (m *mockStorageHandler) WriteBucket(bucketID int, storageID int, readBucketBlocks map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error) {
	return m.writeBucketFunc(bucketID, storageID, readBucketBlocks, shardNodeBlocks)
}

func (m *mockStorageHandler) ReadBlock(bucketID int, storageID int, offset int) (value string, err error) {
	m.readBlockAccessedOffsets = append(m.readBlockAccessedOffsets, offset)
	return "", nil
}

func (m *mockStorageHandler) GetBucketsInPaths(paths []int) (bucketIDs []int, err error) {
	for i := 0; i < m.levelCount; i++ {
		bucketIDs = append(bucketIDs, i)
	}
	return bucketIDs, nil
}

func (m *mockStorageHandler) GetMultipleReverseLexicographicPaths(evictionCount int, count int) (paths []int) {
	for i := 0; i < count; i++ {
		paths = append(paths, 1)
	}
	return paths
}

func (m *mockStorageHandler) GetRandomStorageID() int {
	return 0
}

func startLeaderRaftNodeServer(t *testing.T) *oramNodeServer {
	fsm := newOramNodeFSM()
	raftPort, err := freeport.GetFreePort()
	if err != nil {
		t.Errorf("unable to get free port")
	}
	r, err := startRaftServer(true, "localhost", 0, raftPort, fsm)
	if err != nil {
		t.Errorf("unable to start raft server")
	}
	<-r.LeaderCh() // wait to become the leader
	storageHandler := newMockStorageHandler(2, 4)
	return newOramNodeServer(0, 0, r, fsm, getMockShardNodeClients(), storageHandler, config.Parameters{MaxBlocksToSend: 5, EvictionRate: 4})
}

func (o *oramNodeServer) withFailedShardNodeClients() *oramNodeServer {
	o.shardNodeRPCClients = getFailedMockShardNodeClients()
	return o
}

func (o *oramNodeServer) withMockStorageHandler(storageHandler *mockStorageHandler) *oramNodeServer {
	o.storageHandler = storageHandler
	return o
}

func TestReadAllBucketsReturnsAllTreeBlocks(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(12, 4))
	blocks, err := o.readAllBuckets([]int{0, 1, 2}, 1)
	if err != nil {
		t.Errorf("Expected successful execution of readBucketAllLevels")
	}

	for idx := range []int{0, 1, 2} {
		for i := 4 * idx; i < 4*idx+4; i++ {
			if _, exists := blocks[idx]; !exists {
				t.Errorf("all tree values should be in the blocksFromReadBucket")
			}
		}
	}
}

func TestReadBlocksFromShardNodeReturnsAllShardNodeBlocks(t *testing.T) {
	o := startLeaderRaftNodeServer(t)
	blocks, err := o.readBlocksFromShardNode([]int{4, 7, 11}, 1, o.shardNodeRPCClients[0])
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

func TestWriteBackBlocksToAllBucketsPushesReceivedBlocksToTree(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(3, 4))
	receivedBlocksIsWritten, err := o.writeBackBlocksToAllBuckets([]int{0, 1, 2},
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
			t.Errorf("expected IsWritten to be true for block %s", block)
		}
	}
}

func TestWriteBackBlocksToAllBucketsReturnsFalseForNotPushedReceivedBlocks(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(3, 4).withCustomWriteFunc(
		func(_ int, _ int, _ map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error) {
			writtenBlocks = make(map[string]string)
			for block, val := range shardNodeBlocks {
				if block != "a" {
					writtenBlocks[block] = val
				}
			}
			return writtenBlocks, nil
		},
	))
	receivedBlocksIsWritten, err := o.writeBackBlocksToAllBuckets([]int{0, 1, 2},
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
	o.evict(0)
	o.oramNodeFSM.unfinishedEvictionMu.Lock()
	defer o.oramNodeFSM.unfinishedEvictionMu.Unlock()

	if o.oramNodeFSM.unfinishedEviction != nil {
		t.Errorf("evict should remove unfinished eviction after successful eviction")
	}
}

func TestEvictKeepsBeginEvictionInFailureScenario(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withFailedShardNodeClients()
	o.evict(0)
	o.oramNodeFSM.unfinishedEvictionMu.Lock()
	defer o.oramNodeFSM.unfinishedEvictionMu.Unlock()
	if o.oramNodeFSM.unfinishedEviction == nil {
		t.Errorf("evict should add an unfinished eviction to FSM in failure scenarios")
	}
}

func TestEvictResetsReadPathCounter(t *testing.T) {
	o := startLeaderRaftNodeServer(t)
	o.evict(0)
	o.readPathCounterMu.Lock()
	defer o.readPathCounterMu.Unlock()
	if o.readPathCounter != 0 {
		t.Errorf("Evict should reset readPathCounter after successful execution")
	}
}

func TestGetDistinctPathsInBatchReturnsDistinctPathsInRequests(t *testing.T) {
	o := newOramNodeServer(0, 0, &raft.Raft{}, &oramNodeFSM{}, make(map[int]ReplicaRPCClientMap), &strg.StorageHandler{}, config.Parameters{})
	paths := o.getDistinctPathsInBatch(
		[]*oramnode.BlockRequest{
			{Block: "a", Path: 1},
			{Block: "b", Path: 1},
			{Block: "c", Path: 2},
			{Block: "d", Path: 3},
			{Block: "e", Path: 2},
			{Block: "f", Path: 1},
		},
	)
	sort.Ints(paths)

	expectedPaths := []int{1, 2, 3}
	if len(paths) != 3 {
		t.Errorf("expected 3 unique paths")
	}
	for i, path := range paths {
		if path != expectedPaths[i] {
			t.Errorf("expected path %d but got %d", expectedPaths[i], path)
		}
	}
}

func TestReadPathIncrementsReadPathCounter(t *testing.T) {
	o := startLeaderRaftNodeServer(t).withMockStorageHandler(newMockStorageHandler(4, 4))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	o.ReadPath(ctx, &oramnode.ReadPathRequest{StorageId: 2, Requests: []*oramnode.BlockRequest{{Block: "a", Path: 1}}})
	if o.readPathCounter != 1 {
		t.Errorf("ReadPath should increment readPathCounter")
	}
}
