package shardnode

import (
	"testing"
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
