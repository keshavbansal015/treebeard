package oramnode

import "testing"

func TestHandleOffsetListReplicationCommandAddsNewOffsetList(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.offsetListMap["block1"] = []int{0, 0, 1, 1}
	fsm.handleOffsetListReplicationCommand("block2", []int{0, 0, 0, 0})
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if _, exists := fsm.offsetListMap["block2"]; !exists {
		t.Errorf("Expected block2 to have offsetList but not found")
	}
	for _, el := range fsm.offsetListMap["block2"] {
		if el != 0 {
			t.Errorf("Expected offset list to have 0 offsets but got: %d", el)
		}
	}
}

func TestHandleOffsetListReplicationCommandKeepsOtherOffsetLists(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.offsetListMap["block1"] = []int{0, 0, 1, 1}
	fsm.handleOffsetListReplicationCommand("block2", []int{0, 0, 0, 0})
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if _, exists := fsm.offsetListMap["block1"]; !exists {
		t.Errorf("Expected block1 to have offsetList but not found")
	}
}

func TestHandleDeleteOffsetListReplicationCommandRemovesElementFromOffsetListMap(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.offsetListMap["block1"] = []int{0, 0, 1, 1}
	fsm.handleDeleteOffsetListReplicationCommand("block1")
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if _, exists := fsm.offsetListMap["block1"]; exists {
		t.Errorf("DeleteOffsetListReplicationCommand should delete offset list")
	}
}

func TestHandleBeginEvictionCommandAddsUnfinishedEviction(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.unfinishedEviction = &beginEvictionData{path: 1, storageID: 2}
	fsm.handleBeginEvictionCommand(32, 54)
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if fsm.unfinishedEviction.path != 32 {
		t.Errorf("Expected an unfinished eviction with path 32 but found path equal to: %d", fsm.unfinishedEviction.path)
	}
	if fsm.unfinishedEviction.storageID != 54 {
		t.Errorf("Expected an unfinished eviction with storageID 54 but found path equal to: %d", fsm.unfinishedEviction.storageID)
	}
}

func TestHandleEndEvictionCommandRemovesUnfinishedEviction(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.unfinishedEviction = &beginEvictionData{path: 1, storageID: 2}
	fsm.handleEndEvictionCommand()
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if fsm.unfinishedEviction != nil {
		t.Errorf("handleEndEvictionCommand should empty unfinished evictions")
	}
}
