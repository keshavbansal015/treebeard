package main

import (
	"math/rand"
	"time"
	"strconv"
)

const bucketSize int = 10
const realBlocks int = 4
const dummyBlocks int = 6

type bucket struct {
	id            int
	blockIndexMap []int
	nextDummy     int
	info          *client
}

func NewBucket(id int, info *client) *bucket {
	blockIndexMap := make([]int, bucketSize)
	for i := 0; i < bucketSize; i++ {
		blockIndexMap[i] = -1
	}
	bucket := &bucket{
		id:            id,
		blockIndexMap: blockIndexMap,
		nextDummy:     realBlocks,
		info:          info,
	}
	bucket.shuffleBlockIndexMap()
	return bucket
}

func (bucket *bucket) shuffleBlockIndexMap() (err error) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(bucket.blockIndexMap), func(i, j int) {
		bucket.blockIndexMap[i], bucket.blockIndexMap[j] = bucket.blockIndexMap[j], bucket.blockIndexMap[i]
	})
	err = bucket.pushIndexMap()
	if err != nil {
		return err
	}
	return nil
}

func (bucket *bucket) getIndex(blockNum int) (index int) {
	if blockNum >= realBlocks {
		// throw error: the value you try to access is a dummy value
	} else if blockNum >= bucketSize {
		// throw error: the value you try to access is out of bound
	}
	return bucket.blockIndexMap[blockNum]
}

func (bucket *bucket) getDummyIndex() (index int) {
	bucket.nextDummy++
	if bucket.nextDummy == bucketSize {
		// need eviction?
	}
	return bucket.blockIndexMap[bucket.nextDummy-1]
}

func (bucket *bucket) pushIndexMap() (err error) {
	strings := make([]string, len(bucket.blockIndexMap))
	for i, num := range bucket.blockIndexMap {
		strings[i] = strconv.Itoa(num)
	}
	err = bucket.info.PushContent(bucket.id, strings)
	if err != nil {
		return err
	}
	return nil
}
