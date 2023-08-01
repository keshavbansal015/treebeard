package oramnode

import "testing"

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
