package shardnode

import (
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

// This test only checks the functionality of the handleReplicateResponse method;
// It doesn't run the local replica handler go routine so it doesn't affect the test results.
// This is achieved by giving an anonymous empty function to the handleReplicateResponse method.
func TestHandleReplicateResponseWithoutPreviousValue(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	payload := createTestReplicateResponsePayload("block", "response", "value", Read)
	shardNodeFSM.handleReplicateResponse("request2", payload, func(requestID string, r ReplicateResponsePayload) {})
	if shardNodeFSM.responseMap["request2"] != "response" {
		t.Errorf("Expected response to be \"response\" for request1, but the value is %s", shardNodeFSM.responseMap["request2"])
	}
}

func TestHandleReplicateResponseOverridingPreviousValue(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.responseMap["request2"] = "prev"
	payload := createTestReplicateResponsePayload("block", "response", "value", Read)
	shardNodeFSM.handleReplicateResponse("request2", payload, func(requestID string, r ReplicateResponsePayload) {})
	if shardNodeFSM.responseMap["request2"] != "response" {
		t.Errorf("Expected response to be \"response\" for request1, but the value is %s", shardNodeFSM.responseMap["request2"])
	}
}

type responseMessage struct {
	requestID string
	response  string
}

// This helper function gets a map of requestID to its channel.
// It waits for all channels to recieve the determined response.
// It will timeout if at least one channel doesn't recieve the response.
func checkWaitingChannelsHelper(t *testing.T, waitChannels map[string]chan string, expectedResponse string) {
	waitingSet := make(map[string]bool) // keeps the request in the set until a response for request is recieved from channel
	agg := make(chan responseMessage)
	for id, ch := range waitChannels {
		waitingSet[id] = true
		go func(requestID string, c chan string) {
			for msg := range c {
				agg <- responseMessage{requestID: requestID, response: msg}
			}
		}(id, ch)
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
func TestHandleLocalReplicaChangesWhenValueInStashReturnsCorrectReadValueToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel["request1"] = make(chan string)
	shardNodeFSM.responseChannel["request2"] = make(chan string)
	shardNodeFSM.responseChannel["request3"] = make(chan string)
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "response", "value", Read)
	shardNodeFSM.mu.Lock()
	go shardNodeFSM.handleLocalResponseReplicationChanges("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "test_value")
}

func TestHandleLocalReplicaChangesWhenValueInStashReturnsCorrectWriteValueToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel["request1"] = make(chan string)
	shardNodeFSM.responseChannel["request2"] = make(chan string)
	shardNodeFSM.responseChannel["request3"] = make(chan string)
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "response", "value_write", Write)
	shardNodeFSM.mu.Lock()
	go shardNodeFSM.handleLocalResponseReplicationChanges("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "value_write")

	if shardNodeFSM.stash["block"].value != "value_write" {
		t.Errorf("The stash value should be equal to \"value_write\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleLocalReplicaChangesWhenValueNotInStashReturnsResponseToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel["request1"] = make(chan string)
	shardNodeFSM.responseChannel["request2"] = make(chan string)
	shardNodeFSM.responseChannel["request3"] = make(chan string)
	shardNodeFSM.responseMap["request1"] = "response_from_oramnode"

	payload := createTestReplicateResponsePayload("block", "response", "", Read)
	shardNodeFSM.mu.Lock()
	go shardNodeFSM.handleLocalResponseReplicationChanges("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "response_from_oramnode")

	if shardNodeFSM.stash["block"].value != "response_from_oramnode" {
		t.Errorf("The stash value should be equal to \"response_from_oramnode\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleLocalReplicaChangesWhenValueNotInStashReturnsWriteResponseToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeLeader{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel["request1"] = make(chan string)
	shardNodeFSM.responseChannel["request2"] = make(chan string)
	shardNodeFSM.responseChannel["request3"] = make(chan string)
	shardNodeFSM.responseMap["request1"] = "response_from_oramnode"

	payload := createTestReplicateResponsePayload("block", "response", "write_val", Write)
	shardNodeFSM.mu.Lock()
	go shardNodeFSM.handleLocalResponseReplicationChanges("request1", payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "write_val")

	if shardNodeFSM.stash["block"].value != "write_val" {
		t.Errorf("The stash value should be equal to \"write_val\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleLocalReplicaChangesWhenNotLeaderDoesNotWriteOnChannels(t *testing.T) {
	shardNodeFSM := newShardNodeFSM()
	shardNodeFSM.raftNode = &mockRaftNodeFollower{}
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2"}
	shardNodeFSM.responseChannel["request1"] = make(chan string)
	shardNodeFSM.responseChannel["request2"] = make(chan string)
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "response", "", Read)
	shardNodeFSM.mu.Lock()
	go shardNodeFSM.handleLocalResponseReplicationChanges("request1", payload)

	for {
		select {
		case <-shardNodeFSM.responseChannel["request1"]:
			t.Errorf("The followers in the raft cluster should not send messages on channels!")
		case <-shardNodeFSM.responseChannel["request2"]:
			t.Errorf("The followers in the raft cluster should not send messages on channels!")
		case <-time.After(1 * time.Second):
			return
		}
	}
}
