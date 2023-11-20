package oramnode

import "testing"

func TestHandleBeginEvictionCommandAddsUnfinishedEviction(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.unfinishedEviction = &beginEvictionData{currentEvictionCount: 31, storageID: 2}
	fsm.handleBeginEvictionCommand(34, 54)
	fsm.unfinishedEvictionMu.Lock()
	defer fsm.unfinishedEvictionMu.Unlock()
	if fsm.unfinishedEviction.currentEvictionCount != 34 {
		t.Errorf("Expected an unfinished eviction with currentEvictionCount 34 but found path equal to: %d", fsm.unfinishedEviction.currentEvictionCount)
	}
	if fsm.unfinishedEviction.storageID != 54 {
		t.Errorf("Expected an unfinished eviction with storageID 54 but found path equal to: %d", fsm.unfinishedEviction.storageID)
	}
}

func TestHandleEndEvictionCommandRemovesUnfinishedEviction(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.unfinishedEviction = &beginEvictionData{currentEvictionCount: 31, storageID: 2}
	fsm.handleEndEvictionCommand(35, 2)
	fsm.unfinishedEvictionMu.Lock()
	defer fsm.unfinishedEvictionMu.Unlock()
	if fsm.unfinishedEviction != nil {
		t.Errorf("handleEndEvictionCommand should empty unfinished evictions")
	}
	if fsm.evictionCountMap[2] != 35 {
		t.Errorf("handleEndEvictionCommand should update the eviction count map")
	}
}
