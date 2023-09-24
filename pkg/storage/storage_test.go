package storage

import (
	"testing"
)

func TestGetBucketsInPathsReturnsAllBucketIDsInPath(t *testing.T) {
	s := NewStorageHandler()
	buckets, err := s.GetBucketsInPaths([]int{1})
	expected := []int{4, 2, 1}
	if err != nil {
		t.Errorf("expected no erros in GetBucketsInPaths")
	}
	for i, bucket := range buckets {
		if bucket != expected[i] {
			t.Errorf("expected bucketID to be %d, but got %d", expected[i], bucket)
		}
	}
}
