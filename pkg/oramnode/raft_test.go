package oramnode

import "testing"

func TestHandleBeginEvictionCommandAddsUnfinishedEviction(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.unfinishedEviction = &beginEvictionData{paths: []int{1, 2, 3}, storageID: 2}
	fsm.handleBeginEvictionCommand([]int{4, 5, 6}, 54)
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	for idx, path := range []int{4, 5, 6} {
		if fsm.unfinishedEviction.paths[idx] != path {
			t.Errorf("Expected an unfinished eviction with path %d but found path equal to: %d", path, fsm.unfinishedEviction.paths[idx])
		}
	}
	if fsm.unfinishedEviction.storageID != 54 {
		t.Errorf("Expected an unfinished eviction with storageID 54 but found path equal to: %d", fsm.unfinishedEviction.storageID)
	}
}

func TestHandleEndEvictionCommandRemovesUnfinishedEviction(t *testing.T) {
	fsm := newOramNodeFSM()
	fsm.unfinishedEviction = &beginEvictionData{paths: []int{1, 2, 3}, storageID: 2}
	fsm.handleEndEvictionCommand()
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if fsm.unfinishedEviction != nil {
		t.Errorf("handleEndEvictionCommand should empty unfinished evictions")
	}
}
