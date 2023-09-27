package storage

import (
	"testing"
)

func TestGetBucketsInPathsReturnsAllBucketIDsInPath(t *testing.T) {
	s := NewStorageHandler()
	buckets, err := s.GetBucketsInPaths([]int{1})
	expectedMap := map[int]bool{
		4: true,
		2: true,
		1: true,
	}
	if err != nil {
		t.Errorf("expected no erros in GetBucketsInPaths")
	}
	for _, bucket := range buckets {
		if _, exists := expectedMap[bucket]; !exists {
			t.Errorf("expected bucketID %d to exist", bucket)
		}
	}
}
