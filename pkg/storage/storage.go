package storage

// TODO: It might need to handle multiple storage shards.

import (
	"context"
	"strconv"
	"errors"
)

// MaxAccessCount is the maximum times we can access a bucket safely.
const (
	MaxAccessCount int = 8
	Z = 4
    S = 6
	shift = 1 // 2^shift children per node
)

// StorageHandler is responsible for handling one or multiple storage shards.
type StorageHandler struct {
	maxAccessCount int
	host string
	db   int
	key  []byte
}

func NewStorageHandler(host string, db int, key []byte) *StorageHandler {
	return &StorageHandler{
		maxAccessCount: MaxAccessCount,
		host: host,
		db:   db,
		key:  key,
	}
}

func (s *StorageHandler) GetMaxAccessCount() int {
	return s.maxAccessCount
}

// It returns valid randomly chosen path and storageID.
func (s *StorageHandler) GetRandomPathAndStorageID() (path int, storageID int) {
	// TODO: implement
	return 0, 0
}

// It returns a block offset based on the blocks argument.
//
// If a real block is found, it returns isReal=true and the block id.
// If non of the "blocks" are in the bucket, it returns isReal=false
func (s *StorageHandler) GetBlockOffset(bucketID int, storageID int, blocks []string) (offset int, isReal bool, blockFound string, err error) {
	// TODO: implement
	blockMap := make(map[string]int)
	for i := 0; i < Z; i++ {
		pos, key, err := s.GetMetadata(bucketID, strconv.Itoa(i))
		if err != nil {
			return -1, false, "", err
		}
		blockMap[key] = pos
	}
	for _, block := range(blocks) {
		pos, exist := blockMap[block]
		if exist {
			return pos, true, block, nil
		}
	}
	return -1, false, "", err
}

// It returns the number of times a bucket was accessed.
// This is helpful to know when to do an early reshuffle.
func (s *StorageHandler) GetAccessCount(bucketID int, storageID int) (count int, err error) {
	client := s.getClient()
	ctx := context.Background()
	accessCountS, err := client.HGet(ctx, strconv.Itoa(-1*bucketID), "accessCount").Result()
	if err != nil {
		return 0, err
	}
	accessCount, err := strconv.Atoi(accessCountS)
	if err != nil {
		return 0, err
	}
	return accessCount, nil
}

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of block id to block values.
func (s *StorageHandler) ReadBucket(bucketID int, storageID int) (blocks map[string]string, err error) {
	// TODO: implement
	client := s.getClient()
	ctx := context.Background()
	blocks = make(map[string]string)
	i := 0
	bit := 0
	for ; i < Z; {
		pos, key, err := s.GetMetadata(bucketID, strconv.Itoa(bit))
		if err != nil {
			return nil, err
		}
		value, err := client.HGet(ctx, strconv.Itoa(bucketID), strconv.Itoa(pos)).Result()
		if err != nil {
			return nil, err
		}
		if value != "__null__" {
			value, err = Decrypt(value, s.key)
			blocks[key] = value
			i++
		}
		bit++
	}
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

// WriteBucket writes readBucketBlocks and shardNodeBlocks to the storage shard.
// It priorotizes readBucketBlocks to shardNodeBlocks.
// It returns the blocks that were written into the storage shard in the writtenBlocks variable.
func (s *StorageHandler) WriteBucket(bucketID int, storageID int, readBucketBlocks map[string]string, shardNodeBlocks map[string]string, isAtomic bool) (writtenBlocks map[string]string, err error) {
	// TODO: implement
	// TODO: It should make the counter zero
	client := s.getClient()
	ctx := context.Background()
	writtenBlocks = make(map[string]string)
	bit := 0
	for ;bit < Z; bit++ {
		pos, _, err := s.GetMetadata(bucketID, strconv.Itoa(bit))
		value, err := client.HGet(ctx, strconv.Itoa(bucketID), strconv.Itoa(pos)).Result()
		if err != nil {
			return nil, err
		}
		if value == "__null__" {
			if len(readBucketBlocks) != 0 {
				for block, value := range readBucketBlocks {
					err = client.HSet(ctx, strconv.Itoa(-1 * bucketID), strconv.Itoa(bit), strconv.Itoa(pos) + block).Err()
					if err != nil {
						return nil, err
					}
					encryptVal, err := Encrypt(value, s.key)
					if err != nil {
						return nil, err
					}
					err = client.HSet(ctx, strconv.Itoa(bucketID), strconv.Itoa(pos), encryptVal).Err()
					if err != nil {
						return nil, err
					}
					writtenBlocks[block] = value
					delete(readBucketBlocks, block)
					break
				}
			} else if len(shardNodeBlocks) != 0 {
				for block, value := range shardNodeBlocks {
					err = client.HSet(ctx, strconv.Itoa(-1 * bucketID), strconv.Itoa(bit), strconv.Itoa(pos) + block).Err()
					if err != nil {
						return nil, err
					}
					encryptVal, err := Encrypt(value, s.key)
					if err != nil {
						return nil, err
					}
					err = client.HSet(ctx, strconv.Itoa(bucketID), strconv.Itoa(pos), encryptVal).Err()
					if err != nil {
						return nil, err
					}
					writtenBlocks[block] = value
					delete(readBucketBlocks, block)
					break
				}
			}
		}
	}
	return writtenBlocks, nil
}

// ReadBlock reads a single block using an its offset.
func (s *StorageHandler) ReadBlock(bucketID int, storageID int, offset int) (value string, err error) {
	// TODO: it should invalidate and increase counter
	client := s.getClient()
	ctx := context.Background()
	value, err = client.HGet(ctx, strconv.Itoa(bucketID), strconv.Itoa(offset)).Result()
	if err != nil {
		return "", err
	}
	if value == "__null__" {
		err = errors.New("you are accessing invalidate value")
		return "", err
	}
	// decode value
	value, err = Decrypt(value, s.key)
	if err != nil {
		return "", err
	}
	// invalidate value (set it to null)
	err = client.HSet(ctx, strconv.Itoa(bucketID), strconv.Itoa(offset), "__null__").Err()
	if err != nil {
		return "", err
	}
	// increment access count
	accessCountS, err := client.HGet(ctx, strconv.Itoa(-1*bucketID), "accessCount").Result()
	if err != nil {
		return "", err
	}
	accessCount, err := strconv.Atoi(accessCountS)
	if err != nil {
		return "", err
	}
	err = client.HSet(ctx, strconv.Itoa(-1*bucketID), "accessCount", accessCount+1).Err()
	if err != nil {
		return "", err
	}
	return value, nil
}

// GetBucketsInPaths return all the bucket ids for the passed paths.
func (s *StorageHandler) GetBucketsInPaths(paths []int) (bucketIDs []int, err error) {
	buckets := make(IntSet)
	for i := 0; i < len(paths); i++ {
		for bucketId := paths[i]; bucketId > 0; bucketId = bucketId >> shift {
			if buckets.Contains(bucketId) {
				break;
			} else {
				buckets.Add(bucketId)
			}
		}
	}
	bucketIDs = make([]int, len(buckets))
	i := 0
	for key := range buckets {
		bucketIDs[i] = key
		i++
	}
	return bucketIDs, nil
}
