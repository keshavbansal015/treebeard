package storage

import (
	"strconv"
	"testing"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
)

func TestGetBucketsInPathsReturnsAllBucketIDsInPath(t *testing.T) {
	s := NewStorageHandler(3, 9, 1, 1, []config.RedisEndpoint{})
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

// test if GetBlockOffset return the right output and test ReadBlock to see if the output offset gets us the desired value
func TestGetBlockOffset(t *testing.T) {
	bucketId := 1
	storageId := 0
	expectedFound := "user1"
	expectedValue := "value1"

	s := NewStorageHandler(3, 1, 9, 1, []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}})
	s.InitDatabase()
	s.WriteBucket(1, 0, map[string]string{"user1": "value1"}, map[string]string{}, true)

	offset, isReal, blockFound, err := s.GetBlockOffset(bucketId, storageId, []string{"user8", "user10", expectedFound})
	if err != nil {
		t.Errorf("expected no erros in GetBlockOffset")
	}
	if blockFound != expectedFound {
		t.Errorf("expecting %s, but found %s", expectedFound, blockFound)
	}
	if !isReal {
		t.Errorf("expected real block in bucket %d", bucketId)
	}
	val, err := s.ReadBlock(bucketId, storageId, offset)
	if val != expectedValue {
		t.Errorf("expecting %s, but found %s", expectedValue, val)
	}
	if err != nil {
		t.Errorf("expected no erros in ReadBlock %s", err)
	}
}

// test ReadBlock and GetAccessCount
func TestReadBlock(t *testing.T) {
	bucketId := 4
	storageId := 0
	s := NewStorageHandler(3, 1, 9, 1, []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}})
	s.InitDatabase()
	for i := 0; i < s.Z+s.S; i++ {
		val, err := s.ReadBlock(bucketId, storageId, i)
		if err != nil {
			t.Errorf("error reading block")
		}
		if val != "value"+strconv.Itoa(bucketId) && val != "b"+strconv.Itoa(bucketId)+"d"+strconv.Itoa(i) {
			t.Errorf("wrong value!")
		}
		accessCount, err := s.GetAccessCount(bucketId, storageId)
		if err != nil {
			t.Errorf("error reading access count")
		}
		if accessCount != i+1 {
			t.Errorf("Incorrect access count: %d", accessCount)
		}
	}
}

func TestWriteBucketBlock(t *testing.T) {
	bucketId := 1
	storageId := 0
	s := NewStorageHandler(3, 1, 9, 1, []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}})
	s.InitDatabase()
	expectedWrittenBlocks := map[string]string{"user1": "value1"}
	writtenBlocks, _ := s.WriteBucket(bucketId, storageId, map[string]string{"user1": "value1"}, map[string]string{"user10": "value10"}, true)
	for block := range writtenBlocks {
		if _, exist := expectedWrittenBlocks[block]; !exist {
			t.Errorf("%s was written", block)
		}
	}
}

func TestGetMultipleRandomPathAndStorageIDReturnsCountUniquePaths(t *testing.T) {
	paths, storageID := GetMultipleRandomPathAndStorageID(3, 2, 2)
	if len(paths) != 2 {
		t.Errorf("expected 2 random paths but got %v", paths)
	}
	for _, path := range paths {
		if path < 1 || path > 4 {
			t.Errorf("expected path to be between 1 and 4")
		}
	}
	if storageID < 0 || storageID > 1 {
		t.Errorf("expected storageID to be between 0 and 1")
	}
}

func TestGetMultipleRandomPathAndStorageIDReturnsAtMostCountPaths(t *testing.T) {
	paths, _ := GetMultipleRandomPathAndStorageID(3, 2, 5)
	if len(paths) != 4 {
		t.Errorf("expected 4 random paths but got %v", paths)
	}
}
