package shardnode

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/hashicorp/raft"
	"github.com/phayes/freeport"
	"google.golang.org/grpc/metadata"
)

func TestGetPathAndStorageBasedOnRequestWhenInitialRequestReturnsRealPathAndStorage(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), nil)
	s.shardNodeFSM.requestLog["block1"] = []string{"request1", "request2"}
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 23, storageID: 3}

	path, storageID := s.getPathAndStorageBasedOnRequest("block1", "request1")
	if path != 23 {
		t.Errorf("Expected path to be a real value from position map equal to 23 but the value is: %d", path)
	}
	if storageID != 3 {
		t.Errorf("Expected storageID to be a real value from position map equal to 3 but the value is: %d", storageID)
	}
}

func TestCreateResponseChannelForRequestIDAddsChannelToResponseChannel(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), nil)
	s.createResponseChannelForRequestID("req1")
	if _, exists := s.shardNodeFSM.responseChannel["req1"]; !exists {
		t.Errorf("Expected a new channel for key req1 but nothing found!")
	}
}

func TestQueryReturnsErrorForNonLeaderRaftPeer(t *testing.T) {
	s := newShardNodeServer(0, 0, &raft.Raft{}, newShardNodeFSM(), nil)
	_, err := s.query(context.Background(), Read, "block", "")
	if err == nil {
		t.Errorf("A non-leader raft peer should return error after call to query.")
	}
}

func cleanRaftDataDirectory(directoryPath string) {
	os.RemoveAll(directoryPath)
}

func getMockOramNodeClients() map[int]ReplicaRPCClientMap {
	return map[int]ReplicaRPCClientMap{
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
}

func startLeaderRaftNodeServer(t *testing.T) *shardNodeServer {
	cleanRaftDataDirectory("data-replicaid-0")

	fsm := newShardNodeFSM()
	raftPort, err := freeport.GetFreePort()
	if err != nil {
		t.Errorf("unable to get free port")
	}
	r, err := startRaftServer(true, 0, raftPort, fsm)
	if err != nil {
		t.Errorf("unable to start raft server")
	}
	fsm.mu.Lock()
	fsm.raftNode = r
	fsm.mu.Unlock()
	<-r.LeaderCh() // wait to become the leader
	return newShardNodeServer(0, 0, r, fsm, getMockOramNodeClients())
}

func TestQueryReturnsResponseRecievedFromOramNode(t *testing.T) {
	s := startLeaderRaftNodeServer(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	response, err := s.query(ctx, Read, "block1", "")
	if response != "response_from_leader" {
		t.Errorf("expected the response to be \"response_from_leader\" but it is: %s", response)
	}
	if err != nil {
		t.Errorf("expected no error in call to query")
	}
}

func TestQueryReturnsSentValueForWriteRequests(t *testing.T) {
	s := startLeaderRaftNodeServer(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	response, err := s.query(ctx, Write, "block1", "val")
	if response != "val" {
		t.Errorf("expected the response to be \"val\" but it is: %s", response)
	}
	if err != nil {
		t.Errorf("expected no error in call to query")
	}
}

func TestQueryCleansTempValuesInFSMAfterExecution(t *testing.T) {
	s := startLeaderRaftNodeServer(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.query(ctx, Write, "block1", "val")
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()
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
	if _, exists := s.shardNodeFSM.responseChannel["request1"]; exists {
		t.Errorf("query should remove the request from the responseChannel after successful execution.")
	}
}

func TestQueryAddsReadValueToStash(t *testing.T) {
	s := startLeaderRaftNodeServer(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.query(ctx, Read, "block1", "")
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()
	if s.shardNodeFSM.stash["block1"].value != "response_from_leader" {
		t.Errorf("The response from the oramnode should be added to the stash")
	}
}

func TestQueryAddsWriteValueToStash(t *testing.T) {
	s := startLeaderRaftNodeServer(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.query(ctx, Write, "block1", "valW")
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()
	if s.shardNodeFSM.stash["block1"].value != "valW" {
		t.Errorf("The write value should be added to the stash")
	}
}

func TestQueryUpdatesPositionMap(t *testing.T) {
	s := startLeaderRaftNodeServer(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", "request1"))
	s.shardNodeFSM.positionMap["block1"] = positionState{path: 13423432, storageID: 3223113}
	s.query(ctx, Write, "block1", "valW")
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()
	if s.shardNodeFSM.positionMap["block1"].path == 13423432 || s.shardNodeFSM.positionMap["block1"].storageID == 3223113 {
		t.Errorf("position map should get updated after request")
	}
}

func TestQueryReturnsResponseToAllWaitingRequests(t *testing.T) {
	s := startLeaderRaftNodeServer(t)
	responseChannel := make(chan string)
	for i := 0; i < 3; i++ {
		go func(idx int) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("requestid", fmt.Sprintf("request%d", idx)))
			response, _ := s.query(ctx, Read, "block1", "")
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
