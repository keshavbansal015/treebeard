package storage

// TODO: It might need to handle multiple storage shards.

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Path and bucket id start from one.

// StorageHandler is responsible for handling one or multiple storage shards.
type StorageHandler struct {
	treeHeight int
	Z          int
	S          int
	shift      int
	storages   map[int]*redis.Client // map of storage id to redis client
	storageMus map[int]*sync.Mutex   // map of storage id to mutex
	key        []byte
}

func NewStorageHandler(treeHeight int, Z int, S int, shift int, redisEndpoints []config.RedisEndpoint) *StorageHandler { // map of storage id to storage info
	log.Debug().Msgf("Creating a new storage handler")
	storages := make(map[int]*redis.Client)
	for _, endpoint := range redisEndpoints {
		storages[endpoint.ID] = getClient(endpoint.IP, endpoint.Port)
	}
	storageMus := make(map[int]*sync.Mutex)
	for storageID := range storages {
		storageMus[storageID] = &sync.Mutex{}
	}
	storageLatestEviction := make(map[int]int)
	for _, endpoint := range redisEndpoints {
		storageLatestEviction[endpoint.ID] = 0
	}
	s := &StorageHandler{
		treeHeight: treeHeight,
		Z:          Z,
		S:          S,
		shift:      shift,
		storages:   storages,
		storageMus: storageMus,
		key:        []byte("passphrasewhichneedstobe32bytes!"),
	}
	return s
}

func (s *StorageHandler) GetMaxAccessCount() int {
	return s.S
}

func (s *StorageHandler) LockStorage(storageID int) {
	log.Debug().Msgf("Aquiring lock for storage %d", storageID)
	s.storageMus[storageID].Lock()
	log.Debug().Msgf("Aquired lock for storage %d", storageID)
}

func (s *StorageHandler) UnlockStorage(storageID int) {
	log.Debug().Msgf("Releasing lock for storage %d", storageID)
	s.storageMus[storageID].Unlock()
	log.Debug().Msgf("Released lock for storage %d", storageID)
}

func (s *StorageHandler) InitDatabase() error {
	log.Debug().Msgf("Initializing the redis database")
	for _, client := range s.storages {
		err := client.FlushAll(context.Background()).Err()
		if err != nil {
			return err
		}
		err = s.databaseInit(client)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO: BatchGetBlockOffset(buckets []int, storageID int, blocks []string) (offsets map[int]int, isReal map[int]int, blockFound ma[int]int, err error)

// It returns a block offset based on the blocks argument.
//
// If a real block is found, it returns isReal=true and the block id.
// If non of the "blocks" are in the bucket, it returns isReal=false
func (s *StorageHandler) GetBlockOffset(bucketID int, storageID int, blocks []string) (offset int, isReal bool, blockFound string, err error) {
	log.Debug().Msgf("Getting block offset for bucket %d and storage %d", bucketID, storageID)
	blockMap := make(map[string]int)
	for i := 0; i < s.Z; i++ {
		pos, key, err := s.GetMetadata(bucketID, strconv.Itoa(i), storageID)
		if err != nil {
			return -1, false, "", err
		}
		blockMap[key] = pos
	}
	for _, block := range blocks {
		pos, exist := blockMap[block]
		if exist {
			log.Debug().Msgf("Found block %s in bucket %d", block, bucketID)
			return pos, true, block, nil
		}
	}
	log.Debug().Msgf("Did not find any block in bucket %d", bucketID)
	return -1, false, "", err
}

// TODO: BatchGetAccessCount(buckets []int, storageID int) (counts map[int]int, err error)
// It returns the number of times a bucket was accessed for multiple buckets.

// It returns the number of times a bucket was accessed.
// This is helpful to know when to do an early reshuffle.
func (s *StorageHandler) GetAccessCount(bucketID int, storageID int) (count int, err error) {
	log.Debug().Msgf("Getting access count for bucket %d and storage %d", bucketID, storageID)
	client := s.storages[storageID]
	ctx := context.Background()
	accessCountS, err := client.HGet(ctx, strconv.Itoa(-1*bucketID), "accessCount").Result()
	if err != nil {
		return 0, err
	}
	accessCount, err := strconv.Atoi(accessCountS)
	if err != nil {
		return 0, err
	}
	log.Debug().Msgf("Access count for bucket %d and storage %d is %d", bucketID, storageID, accessCount)
	return accessCount, nil
}

// TODO: BatchReadBucket(buckets []int, storageID int) (blocks map[int]map[string]string, err error)
// It reads multiple buckets from a single storage shard.

// ReadBucket reads exactly Z blocks from the bucket.
// It reads all the valid real blocks and random vaid dummy blocks if the bucket contains less than Z valid real blocks.
// blocks is a map of block id to block values.
func (s *StorageHandler) ReadBucket(bucketID int, storageID int) (blocks map[string]string, err error) {
	log.Debug().Msgf("Reading bucket %d from storage %d", bucketID, storageID)
	client := s.storages[storageID]
	ctx := context.Background()
	blocks = make(map[string]string)
	i := 0
	bit := 0
	for i < s.Z {
		pos, key, err := s.GetMetadata(bucketID, strconv.Itoa(bit), storageID)
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

// TODO: WriteBucket(buckets []int, storageID int, readBucketBlocks map[int]map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error)
// It writes blocks to multiple buckets in a single storage shard.

// WriteBucket writes readBucketBlocks and shardNodeBlocks to the storage shard.
// It priorotizes readBucketBlocks to shardNodeBlocks.
// It returns the blocks that were written into the storage shard in the writtenBlocks variable.
func (s *StorageHandler) WriteBucket(bucketID int, storageID int, readBucketBlocks map[string]string, shardNodeBlocks map[string]string) (writtenBlocks map[string]string, err error) {
	log.Debug().Msgf("Writing bucket %d to storage %d", bucketID, storageID)
	// TODO: It should make the counter zero
	values := make([]string, s.Z+s.S)
	metadatas := make([]string, s.Z+s.S)
	realIndex := make([]int, s.Z+s.S)
	for k := 0; k < s.Z+s.S; k++ {
		// Generate a random number between 0 and 9
		realIndex[k] = k
	}
	shuffleArray(realIndex)
	writtenBlocks = make(map[string]string)
	i := 0
	for key, value := range readBucketBlocks {
		if strings.HasPrefix(key, "dummy") {
			continue
		}
		if len(writtenBlocks) < s.Z {
			writtenBlocks[key] = value
			values[realIndex[i]], err = Encrypt(value, s.key)
			if err != nil {
				return nil, err
			}
			metadatas[i] = strconv.Itoa(realIndex[i]) + key
			i++
			// pos_map is updated in server?
		} else {
			break
		}
	}
	for key, value := range shardNodeBlocks {
		if strings.HasPrefix(key, "dummy") {
			continue
		}
		if len(writtenBlocks) < s.Z {
			writtenBlocks[key] = value
			values[realIndex[i]], err = Encrypt(value, s.key)
			if err != nil {
				return nil, err
			}
			metadatas[i] = strconv.Itoa(realIndex[i]) + key
			i++
		} else {
			break
		}
	}
	dummyCount := rand.Intn(1000)
	for ; i < s.Z+s.S; i++ {
		dummyID := "dummy" + strconv.Itoa(dummyCount)
		dummyString := "b" + strconv.Itoa(bucketID) + "d" + strconv.Itoa(i)
		dummyString, err = Encrypt(dummyString, s.key)
		if err != nil {
			log.Error().Msgf("Error encrypting data")
			return nil, err
		}
		// push dummy to array
		values[realIndex[i]] = dummyString
		// push meta data of dummies to array
		metadatas[i] = strconv.Itoa(realIndex[i]) + dummyID
		dummyCount++
	}
	// push content of value array and meta data array
	err = s.PushDataAndMetadata(bucketID, values, metadatas, s.storages[storageID])
	if err != nil {
		log.Error().Msgf("Error pushing values to db: %v", err)
		return nil, err
	}
	return writtenBlocks, nil
}

// TODO: BatchReadBlock(buckets []int,  storageID int, offsets []int) (values map[int]string, err error)
// It reads multiple blocks from multiple buckets and returns the values.

// ReadBlock reads a single block using the offset.
func (s *StorageHandler) ReadBlock(bucketID int, storageID int, offset int) (value string, err error) {
	log.Debug().Msgf("Reading block %d from bucket %d in storage %d", offset, bucketID, storageID)
	client := s.storages[storageID]
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
	log.Debug().Msgf("Getting buckets in paths %v", paths)
	buckets := make(IntSet)
	for i := 0; i < len(paths); i++ {
		leafID := int(math.Pow(2, float64(s.treeHeight-1)) + float64(paths[i]) - 1)
		for bucketId := leafID; bucketId > 0; bucketId = bucketId >> s.shift {
			if buckets.Contains(bucketId) {
				break
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

// It returns valid randomly chosen path and storageID.
func GetRandomPathAndStorageID(treeHeight int, storageCount int) (path int, storageID int) {
	log.Debug().Msgf("Getting random path and storage id")
	paths := int(math.Pow(2, float64(treeHeight-1)))
	randomPath := rand.Intn(paths) + 1
	randomStorage := rand.Intn(storageCount)
	return randomPath, randomStorage
}

func (s *StorageHandler) GetRandomStorageID() int {
	log.Debug().Msgf("Getting random storage id")
	index := rand.Intn(len(s.storages))
	for storageID := range s.storages {
		if index == 0 {
			return storageID
		}
		index--
	}
	return -1
}

func (s *StorageHandler) GetMultipleReverseLexicographicPaths(evictionCount int, count int) (paths []int) {
	log.Debug().Msgf("Getting multiple reverse lexicographic paths")
	paths = make([]int, count)
	for i := 0; i < count; i++ {
		paths[i] = GetNextReverseLexicographicPath(evictionCount, s.treeHeight)
		evictionCount++
	}
	return paths
}

// evictionCount starts from zero and goes forward
func GetNextReverseLexicographicPath(evictionCount int, treeHeight int) (nextPath int) {
	evictionCount = evictionCount % int(math.Pow(2, float64(treeHeight-1)))
	log.Debug().Msgf("Getting next reverse lexicographic path")
	reverseBinary := utils.BinaryReverse(evictionCount, treeHeight-1)
	return reverseBinary + 1
}
