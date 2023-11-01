package storage

import (
	//"fmt"
	"context"
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

	s := NewStorageHandler(3, 9, 1, 1, []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}})
	s.WriteBucket(0, 0, map[string]string{"user1": "value1"}, map[string]string{}, true)

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
		t.Errorf("expected no erros in ReadBlock")
	}
}

// test ReadBlock and GetAccessCount
func TestReadBlock(t *testing.T) {
	bucketId := 4
	storageId := 0
	s := NewStorageHandler(3, 9, 1, 1, []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}})
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
	s := NewStorageHandler(3, 9, 1, 1, []config.RedisEndpoint{{ID: 0, IP: "localhost", Port: 6379}})
	s.storages[0].FlushAll(context.Background())
	expectedWrittenBlocks := map[string]string{"user1": "value1"}
	writtenBlocks, _ := s.WriteBucket(bucketId, storageId, map[string]string{"user1": "value1"}, map[string]string{"user10": "value10"}, true)
	for block := range writtenBlocks {
		if _, exist := expectedWrittenBlocks[block]; !exist {
			t.Errorf("%s was written", block)
		}
	}
}
