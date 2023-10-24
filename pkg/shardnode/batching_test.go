package shardnode

import "testing"

func TestAddRequestToStorageQueueAndWaitAddsRequestAndChannel(t *testing.T) {
	b := newBatchManager(4)
	b.storageQueues[1] = []blockRequest{{block: "b", path: 2}}
	b.addRequestToStorageQueueAndWait(blockRequest{block: "a", path: 2}, 1)
	if len(b.storageQueues[1]) != 2 {
		t.Errorf("request should be added to the correct storage queue")
	}
	if b.storageQueues[1][1].block != "a" || b.storageQueues[1][1].path != 2 {
		t.Errorf("request should be added to the correct storage queue")
	}
	if _, exists := b.responseChannel["a"]; !exists {
		t.Errorf("batchManager should add channel for block a to the responseChannel")
	}
}

func TestDeleteRequestsFromQueueDeletesRequests(t *testing.T) {
	b := newBatchManager(4)
	b.storageQueues[1] = []blockRequest{{block: "b", path: 2}, {block: "a", path: 2}, {block: "c", path: 2}}
	b.deleteRequestsFromQueue(1, 2)
	if len(b.storageQueues[1]) != 1 {
		t.Errorf("request should be deleted from the correct storage queue")
	}
	if b.storageQueues[1][0].block != "c" || b.storageQueues[1][0].path != 2 {
		t.Errorf("requests should be deleted from the start of the queue")
	}
}
