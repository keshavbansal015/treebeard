package shardnode

import (
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func createTestReplicateRequestAndPathAndStoragePayload(block string, path int, storageID int) ReplicateRequestAndPathAndStoragePayload {
	return ReplicateRequestAndPathAndStoragePayload{
		RequestedBlock: block,
		Path:           path,
		StorageID:      storageID,
	}
}

func TestHandleReplicateRequestAndPathAndStorageToEmptyFSM(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	payload := createTestReplicateRequestAndPathAndStoragePayload("block", 11, 12)
	shardNodeFSM.handleReplicateRequestAndPathAndStorage("request1", payload)
	if len(shardNodeFSM.requestLog["block"]) != 1 || shardNodeFSM.requestLog["block"][0] != "request1" {
		t.Errorf("Expected request1 to be in the requestLog, but the array is equal to %v", shardNodeFSM.requestLog["block"])
	}
	if shardNodeFSM.pathMap["request1"] != 11 {
		t.Errorf("Expected path for request1 to be equal to 11, but the path is equal to %d", shardNodeFSM.pathMap["request1"])
	}
	if shardNodeFSM.storageIDMap["request1"] != 12 {
		t.Errorf("Expected storage id for request1 to be equal to 12, but the storage id is equal to %d", shardNodeFSM.storageIDMap["request1"])
	}
}

func TestHandleReplicateRequestAndPathAndStorageToWithValueFSM(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.requestLog["block"] = []string{"randomrequest"}
	shardNodeFSM.pathMap["request1"] = 20
	shardNodeFSM.storageIDMap["request1"] = 30
	payload := createTestReplicateRequestAndPathAndStoragePayload("block", 11, 12)
	shardNodeFSM.handleReplicateRequestAndPathAndStorage("request1", payload)
	if len(shardNodeFSM.requestLog["block"]) != 2 ||
		shardNodeFSM.requestLog["block"][0] != "randomrequest" ||
		shardNodeFSM.requestLog["block"][1] != "request1" {
		t.Errorf("Expected request1 to be in the second position of requestLog, but the array is equal to %v", shardNodeFSM.requestLog["block"])
	}
	if shardNodeFSM.pathMap["request1"] != 11 {
		t.Errorf("Expected path for request1 to be equal to 11, but the path is equal to %d", shardNodeFSM.pathMap["request1"])
	}
	if shardNodeFSM.storageIDMap["request1"] != 12 {
		t.Errorf("Expected storage id for request1 to be equal to 12, but the storage id is equal to %d", shardNodeFSM.storageIDMap["request1"])
	}
}

func createTestReplicateResponsePayload(block string, response string, value string, op OperationType) ReplicateResponsePayload {
	return ReplicateResponsePayload{
		RequestedBlock: block,
		Response:       response,
		NewValue:       value,
		OpType:         op,
	}
}

type responseMessage struct {
	requestID string
	response  string
}

// This helper function gets a map of requestID to its channel.
// It waits for all channels to recieve the determined response.
// It will timeout if at least one channel doesn't recieve the response.
func checkWaitingChannelsHelper(t *testing.T, waitChannels sync.Map, expectedResponse string) {
	waitingSet := make(map[string]bool) // keeps the request in the set until a response for request is recieved from channel
	agg := make(chan responseMessage)
	var keys []string
	waitChannels.Range(func(key any, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	for _, key := range keys {
		waitingSet[key] = true
		chAny, _ := waitChannels.Load(key)
		go func(requestID string, c chan string) {
			for msg := range c {
				agg <- responseMessage{requestID: requestID, response: msg}
			}
		}(key, chAny.(chan string))
	}
	for {
		if len(waitingSet) == 0 {
			return
		}
		select {
		case val := <-agg:
			if val.response != expectedResponse {
				t.Errorf("The channel for %s got the value %s while it should get %s", val.requestID, val.response, expectedResponse)
			}
			delete(waitingSet, val.requestID)
		case <-time.After(1 * time.Second):
			t.Errorf("timeout occured, failed to recieve the value on channels")
		}
	}
}

type mockRaftNodeLeader struct {
}

func (m *mockRaftNodeLeader) State() raft.RaftState {
	return raft.Leader
}

type mockRaftNodeFollower struct {
}

func (m *mockRaftNodeFollower) State() raft.RaftState {
	return raft.Follower
}

// In this case all the go routines should get the value that resides in stash.
// The stash value has priority over the response value.
func TestHandleReplicateResponseWhenValueInStashReturnsCorrectReadValueToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "response", "value", Read)
	go shardNodeFSM.handleReplicateResponse("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "test_value")
}

func TestHandleReplicateResponseWhenValueInStashReturnsCorrectWriteValueToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "response", "value_write", Write)
	go shardNodeFSM.handleReplicateResponse("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "value_write")

	if shardNodeFSM.stash["block"].value != "value_write" {
		t.Errorf("The stash value should be equal to \"value_write\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleReplicateResponseWhenValueNotInStashReturnsResponseToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))

	payload := createTestReplicateResponsePayload("block", "response_from_oramnode", "", Read)
	go shardNodeFSM.handleReplicateResponse("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "response_from_oramnode")

	if shardNodeFSM.stash["block"].value != "response_from_oramnode" {
		t.Errorf("The stash value should be equal to \"response_from_oramnode\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleReplicateResponseWhenValueNotInStashReturnsWriteResponseToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))

	payload := createTestReplicateResponsePayload("block", "response", "write_val", Write)
	go shardNodeFSM.handleReplicateResponse("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "write_val")

	if shardNodeFSM.stash["block"].value != "write_val" {
		t.Errorf("The stash value should be equal to \"write_val\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleReplicateResponseWhenNotLeaderDoesNotWriteOnChannels(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeFollower{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2"}
	shardNodeFSM.responseChannel.Store("request1", make(chan string))
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "response", "", Read)
	go shardNodeFSM.handleReplicateResponse("request1", payload)

	for {
		ch1Any, _ := shardNodeFSM.responseChannel.Load("request1")
		ch2Any, _ := shardNodeFSM.responseChannel.Load("request2")
		select {
		case <-ch1Any.(chan string):
			t.Errorf("The followers in the raft cluster should not send messages on channels!")
		case <-ch2Any.(chan string):
			t.Errorf("The followers in the raft cluster should not send messages on channels!")
		case <-time.After(1 * time.Second):
			return
		}
	}
}
