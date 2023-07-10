package shardnode

import (
	"testing"

	"github.com/hashicorp/raft"
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
