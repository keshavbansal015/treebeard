package storage

// TODO: It might need to handle multiple storage shards.

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/utils"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Path and bucket id start from one.

// StorageHandler is responsible for handling one or multiple storage shards.
type StorageHandler struct {
	treeHeight int
	Z          int // the maximum number of real blocks in each bucket
	S          int // the number of dummy blocks in each bucket
	shift      int
	storages   map[int]*redis.Client // map of storage id to redis client
	storageMus map[int]*sync.Mutex   // map of storage id to mutex
	key        []byte
}

type BlockInfo struct {
	Value string
	Path  int
}

func NewStorageHandler(treeHeight int, Z int, S int, shift int, redisEndpoints []config.RedisEndpoint) *StorageHandler { // map of storage id to storage info
	log.Info().Msg("Creating a new storage handler.")
	log.Debug().Msgf("Parameters: treeHeight=%d, Z=%d, S=%d, shift=%d", treeHeight, Z, S, shift)
	storages := make(map[int]*redis.Client)
	for _, endpoint := range redisEndpoints {
		log.Debug().Msgf("Connecting to Redis endpoint at %s:%d for storage ID %d", endpoint.IP, endpoint.Port, endpoint.ID)
		storages[endpoint.ID] = getClient(endpoint.IP, endpoint.Port)
	}
	storageMus := make(map[int]*sync.Mutex)
	for storageID := range storages {
		storageMus[storageID] = &sync.Mutex{}
		log.Debug().Msgf("Created mutex for storage ID %d", storageID)
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
	log.Info().Msg("Storage handler created successfully.")
	return s
}

func (s *StorageHandler) GetMaxAccessCount() int {
	return s.S
}

func (s *StorageHandler) LockStorage(storageID int) {
	log.Debug().Msgf("Attempting to acquire lock for storage %d...", storageID)
	s.storageMus[storageID].Lock()
	log.Debug().Msgf("Acquired lock for storage %d.", storageID)
}

func (s *StorageHandler) UnlockStorage(storageID int) {
	log.Debug().Msgf("Attempting to release lock for storage %d...", storageID)
	s.storageMus[storageID].Unlock()
	log.Debug().Msgf("Released lock for storage %d.", storageID)
}

func (s *StorageHandler) InitDatabase() error {
	log.Info().Msg("Initializing the Redis database.")
	for storageID, client := range s.storages {
		log.Debug().Msgf("Checking if database for storage ID %d is already initialized.", storageID)
		// Do not reinitialize the database if it is already initialized
		dbsize, err := client.DBSize(context.Background()).Result()
		if err != nil {
			log.Error().Msgf("Failed to get DB size for storage ID %d: %v", storageID, err)
			return err
		}
		expectedSize := (int64((math.Pow(float64(s.shift+1), float64(s.treeHeight)))) - 1) * 2
		if dbsize == expectedSize {
			log.Info().Msgf("Database for storage ID %d is already initialized with size %d. Skipping initialization.", storageID, dbsize)
			continue
		}
		log.Warn().Msgf("Database for storage ID %d has size %d, expected %d. Flushing and reinitializing.", storageID, dbsize, expectedSize)
		err = client.FlushAll(context.Background()).Err()
		if err != nil {
			log.Error().Msgf("Failed to flush all for storage ID %d: %v", storageID, err)
			return err
		}
		err = s.databaseInit(client)
		if err != nil {
			log.Error().Msgf("Failed to initialize database for storage ID %d: %v", storageID, err)
			return err
		}
		log.Info().Msgf("Successfully initialized database for storage ID %d.", storageID)
	}
	return nil
}

type BlockOffsetStatus struct {
	Offset     int
	IsReal     bool
	BlockFound string
}

func (s *StorageHandler) BatchGetBlockOffset(bucketIDs []int, storageID int, blocks []string) (blockoffsetStatuses map[int]BlockOffsetStatus, err error) {
	log.Debug().Msgf("Starting BatchGetBlockOffset for %d buckets and %d blocks on storage ID %d", len(bucketIDs), len(blocks), storageID)
	allBlockMap, err := s.BatchGetAllMetaData(bucketIDs, storageID)
	if err != nil {
		log.Error().Msgf("Error getting meta data for storage ID %d: %v", storageID, err)
		return nil, err
	}
	log.Debug().Msgf("Successfully retrieved metadata for %d buckets", len(allBlockMap))
	blockoffsetStatuses = make(map[int]BlockOffsetStatus)
	for _, bucketID := range bucketIDs {
		blockoffsetStatuses[bucketID] = BlockOffsetStatus{
			Offset:     -1,
			IsReal:     false,
			BlockFound: "",
		}
		blockMap := allBlockMap[bucketID]
		for _, block := range blocks {
			pos, exist := blockMap[block]
			if exist {
				log.Debug().Msgf("Found block %s at offset %d in bucket %d", block, pos, bucketID)
				blockoffsetStatuses[bucketID] = BlockOffsetStatus{
					Offset:     pos,
					IsReal:     true,
					BlockFound: block,
				}
			}
		}
		if blockoffsetStatuses[bucketID].Offset == -1 {
			log.Debug().Msgf("Did not find any real block in bucket %d. Searching for dummy block.", bucketID)
			if pos, exist := blockMap["dummy1"]; exist {
				log.Debug().Msgf("Found dummy block 'dummy1' at offset %d in bucket %d", pos, bucketID)
				blockoffsetStatuses[bucketID] = BlockOffsetStatus{
					Offset:     pos,
					IsReal:     false,
					BlockFound: "dummy1",
				}
			} else {
				log.Error().Msgf("Did not find valid dummy block 'dummy1' in bucket %d. Returning an error.", bucketID)
				return nil, fmt.Errorf("did not find valid dummy block in bucket %d", bucketID)
			}
		}
	}
	log.Debug().Msgf("Finished BatchGetBlockOffset. Found statuses for %d buckets.", len(blockoffsetStatuses))
	return blockoffsetStatuses, nil
}

// It returns the number of times a bucket was accessed for multiple buckets.
// This is helpful to know when to do an early reshuffle.
func (s *StorageHandler) BatchGetAccessCount(bucketIDs []int, storageID int) (counts map[int]int, err error) {
	log.Debug().Msgf("Starting BatchGetAccessCount for %d buckets on storage ID %d", len(bucketIDs), storageID)
	ctx := context.Background()
	pipe := s.storages[storageID].Pipeline()
	resultsMap := make(map[int]*redis.StringCmd)
	counts = make(map[int]int)
	// Iterate over each bucketID
	for _, bucketID := range bucketIDs {
		// Issue HGET command for the access count of the current bucketID within the pipeline
		cmd := pipe.HGet(ctx, strconv.Itoa(-1*bucketID), "accessCount")
		resultsMap[bucketID] = cmd
		log.Debug().Msgf("Pipelining HGET for accessCount for bucket %d", bucketID)
	}

	// Execute the pipeline for all bucketIDs
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Error().Msgf("Failed to execute pipeline for BatchGetAccessCount on storage ID %d: %v", storageID, err)
		return nil, err
	}
	// Process the results for each bucketID
	for bucketID, cmd := range resultsMap {
		accessCountS, err := cmd.Result()
		if err != nil {
			if err != redis.Nil {
				log.Error().Msgf("Failed to get result for bucket %d: %v", bucketID, err)
				return nil, err
			}
			counts[bucketID] = 0
			log.Debug().Msgf("Access count for bucket %d and storage %d is %d (not found)", bucketID, storageID, 0)
		} else {
			accessCount, err := strconv.Atoi(accessCountS)
			if err != nil {
				log.Error().Msgf("Failed to convert access count to integer for bucket %d: %v", bucketID, err)
				return nil, err
			}
			counts[bucketID] = accessCount
			log.Debug().Msgf("Access count for bucket %d and storage %d is %d", bucketID, storageID, accessCount)
		}
	}

	log.Debug().Msgf("Finished BatchGetAccessCount. Retrieved counts for %d buckets", len(counts))
	return counts, nil
}

// It reads multiple buckets from a single storage shard.
func (s *StorageHandler) BatchReadBucket(bucketIDs []int, storageID int) (blocks map[int]map[string]string, err error) {
	log.Debug().Msgf("Starting BatchReadBucket for %d buckets on storage ID %d", len(bucketIDs), storageID)
	metadataMap, err := s.BatchGetAllMetaData(bucketIDs, storageID)
	if err != nil {
		log.Error().Msgf("Error getting metadata for storage ID %d: %v", storageID, err)
		return nil, err
	}
	log.Debug().Msgf("Successfully retrieved metadata for %d buckets", len(metadataMap))
	results := make(map[int]map[string]*redis.StringCmd)
	pipe := s.storages[storageID].Pipeline()
	ctx := context.Background()
	for bucketID, metadata := range metadataMap {
		i := 0
		results[bucketID] = make(map[string]*redis.StringCmd)
		for key, pos := range metadata {
			if !strings.HasPrefix(key, "dummy") {
				log.Debug().Msgf("Pipelining HGET for real block '%s' at offset %d in bucket %d", key, pos, bucketID)
				results[bucketID][key] = pipe.HGet(ctx, strconv.Itoa(bucketID), strconv.Itoa(pos))
				i++
			}
		}
		dummyCount := 1
		for ; i < s.Z; i++ {
			dummyID := "dummy" + strconv.Itoa(dummyCount)
			pos, exists := metadata[dummyID]
			if !exists {
				log.Error().Msgf("Dummy block '%s' not found in metadata for bucket %d", dummyID, bucketID)
				return nil, fmt.Errorf("dummy block not found")
			}
			log.Debug().Msgf("Pipelining HGET for dummy block '%s' at offset %d to maintain access pattern", dummyID, pos)
			pipe.HGet(ctx, strconv.Itoa(bucketID), strconv.Itoa(pos))
			dummyCount++
		}
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Error().Msgf("Failed to execute pipeline for BatchReadBucket on storage ID %d: %v", storageID, err)
		return nil, err
	}
	blocks = make(map[int]map[string]string)
	for bucketID, result := range results {
		blocks[bucketID] = make(map[string]string)
		for key, cmd := range result {
			value, err := cmd.Result()
			if err != nil {
				log.Error().Msgf("Failed to get result for block '%s' in bucket %d: %v", key, bucketID, err)
				return nil, err
			}
			decryptedValue, err := Decrypt(value, s.key)
			if err != nil {
				log.Error().Msgf("Failed to decrypt value for block '%s' in bucket %d: %v", key, bucketID, err)
				return nil, err
			}
			blocks[bucketID][key] = decryptedValue
			log.Trace().Msgf("Decrypted value for block '%s' in bucket %d: %s", key, bucketID, decryptedValue)
		}
	}
	log.Debug().Msgf("Finished BatchReadBucket. Read blocks from %d buckets.", len(blocks))
	return blocks, nil
}

// creates a map of bucketIDs to blocks that can go in that bucket
func (s *StorageHandler) getBucketToValidBlocksMap(shardNodeBlocks map[string]BlockInfo) map[int][]string {
	log.Debug().Msgf("Starting getBucketToValidBlocksMap with %d blocks from shardnode", len(shardNodeBlocks))
	bucketToValidBlocksMap := make(map[int][]string)
	for key, blockInfo := range shardNodeBlocks {
		leafID := int(math.Pow(2, float64(s.treeHeight-1)) + float64(blockInfo.Path) - 1)
		log.Debug().Msgf("Processing block '%s', path %d, leafID %d", key, blockInfo.Path, leafID)
		for bucketId := leafID; bucketId > 0; bucketId = bucketId >> s.shift {
			bucketToValidBlocksMap[bucketId] = append(bucketToValidBlocksMap[bucketId], key)
			log.Debug().Msgf("Added block '%s' to valid blocks for bucket %d", key, bucketId)
		}
	}
	log.Debug().Msgf("Finished creating map. Total buckets with valid blocks: %d", len(bucketToValidBlocksMap))
	return bucketToValidBlocksMap
}

// It writes blocks to multiple buckets in a single storage shard.
func (s *StorageHandler) BatchWriteBucket(storageID int, readBucketBlocksList map[int]map[string]string, shardNodeBlocks map[string]BlockInfo) (writtenBlocks map[string]string, err error) {
	log.Debug().Msgf("Starting BatchWriteBucket for storage ID %d. %d buckets from read and %d blocks from shardnode.", storageID, len(readBucketBlocksList), len(shardNodeBlocks))
	pipe := s.storages[storageID].Pipeline()
	ctx := context.Background()
	dataResults := make(map[int]*redis.BoolCmd)
	metadataResults := make(map[int]*redis.BoolCmd)
	writtenBlocks = make(map[string]string)

	log.Trace().Msgf("buckets from readBucketBlocksList: %v", readBucketBlocksList)
	log.Trace().Msgf("shardNodeBlocks: %v", shardNodeBlocks)

	bucketToValidBlocksMap := s.getBucketToValidBlocksMap(shardNodeBlocks)
	log.Debug().Msgf("Successfully created bucket-to-valid-blocks map.")

	for bucketID, readBucketBlocks := range readBucketBlocksList {
		log.Debug().Msgf("Preparing to write to bucket %d.", bucketID)
		values := make([]string, s.Z+s.S)
		metadatas := make([]string, s.Z+s.S)
		realIndex := make([]int, s.Z+s.S)
		for k := 0; k < s.Z+s.S; k++ {
			realIndex[k] = k
		}
		shuffleArray(realIndex)
		log.Debug().Msgf("Shuffled indices for bucket %d: %v", bucketID, realIndex)
		i := 0
		for key, value := range readBucketBlocks {
			if strings.HasPrefix(key, "dummy") {
				continue
			}
			if i < s.Z {
				log.Debug().Msgf("Writing real block '%s' from read bucket to offset %d in bucket %d", key, realIndex[i], bucketID)
				writtenBlocks[key] = value
				values[realIndex[i]], err = Encrypt(value, s.key)
				if err != nil {
					log.Error().Msgf("Error encrypting value for block '%s': %v", key, err)
					return nil, err
				}
				metadatas[i] = strconv.Itoa(realIndex[i]) + key
				i++
			} else {
				log.Warn().Msgf("Reached max real blocks for bucket %d. Skipping block '%s'.", bucketID, key)
				break
			}
		}
		for _, key := range bucketToValidBlocksMap[bucketID] {
			if strings.HasPrefix(key, "dummy") {
				continue
			}
			if i < s.Z {
				log.Debug().Msgf("Writing real block '%s' from shardnode to offset %d in bucket %d", key, realIndex[i], bucketID)
				writtenBlocks[key] = shardNodeBlocks[key].Value
				values[realIndex[i]], err = Encrypt(shardNodeBlocks[key].Value, s.key)
				if err != nil {
					log.Error().Msgf("Error encrypting value for block '%s': %v", key, err)
					return nil, err
				}
				metadatas[i] = strconv.Itoa(realIndex[i]) + key
				i++
			} else {
				log.Warn().Msgf("Reached max real blocks for bucket %d. Skipping block '%s' from shardnode.", bucketID, key)
				break
			}
		}
		log.Debug().Msgf("Filled %d real blocks. Now filling %d dummy blocks.", i, (s.Z+s.S)-i)
		dummyCount := 1
		for ; i < s.Z+s.S; i++ {
			dummyID := "dummy" + strconv.Itoa(dummyCount)
			dummyString := "b" + strconv.Itoa(bucketID) + "d" + strconv.Itoa(i)
			dummyString, err = Encrypt(dummyString, s.key)
			if err != nil {
				log.Error().Msgf("Error encrypting dummy data for bucket %d: %v", bucketID, err)
				return nil, err
			}
			values[realIndex[i]] = dummyString
			metadatas[i] = strconv.Itoa(realIndex[i]) + dummyID
			dummyCount++
		}
		log.Debug().Msgf("Pipelining write for data and metadata for bucket %d", bucketID)
		dataResults[i], metadataResults[i] = s.BatchPushDataAndMetadata(bucketID, values, metadatas, pipe)
	}
	log.Debug().Msg("Executing pipeline for all write operations.")
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Error().Msgf("Failed to execute pipeline for BatchWriteBucket: %v", err)
		return nil, err
	}
	log.Debug().Msg("Pipeline execution complete. Checking results.")
	for _, dataCmd := range dataResults {
		_, err := dataCmd.Result()
		if err != nil {
			if err != redis.Nil {
				log.Error().Msgf("Error getting result from data command: %v", err)
				return nil, err
			}
			return nil, err
		}
	}
	for _, metadataCmd := range dataResults {
		_, err := metadataCmd.Result()
		if err != nil {
			if err != redis.Nil {
				log.Error().Msgf("Error getting result from metadata command: %v", err)
				return nil, err
			}
			return nil, err
		}
	}
	log.Debug().Msgf("BatchWriteBucket completed successfully. Written blocks count: %d", len(writtenBlocks))
	return writtenBlocks, nil
}

// It reads multiple blocks from multiple buckets and returns the values.
func (s *StorageHandler) BatchReadBlock(bucketOffsets map[int]int, storageID int) (values map[int]string, err error) {
	log.Debug().Msgf("Starting BatchReadBlock for %d bucket offsets on storage ID %d", len(bucketOffsets), storageID)
	ctx := context.Background()
	pipe := s.storages[storageID].Pipeline()
	resultsMap := make(map[int]*redis.StringCmd)
	for bucketID, offset := range bucketOffsets {
		log.Debug().Msgf("Pipelining HGET for bucket %d, offset %d", bucketID, offset)
		cmd := pipe.HGet(ctx, strconv.Itoa(bucketID), strconv.Itoa(offset))
		resultsMap[bucketID] = cmd
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Error().Msgf("Error executing batch read block pipe: %v", err)
		return nil, err
	}
	log.Debug().Msg("First pipeline executed successfully. Processing results.")
	values = make(map[int]string)
	for bucketID, cmd := range resultsMap {
		block, err := cmd.Result()
		if err != nil && err != redis.Nil {
			log.Error().Msgf("Failed to get result for bucket %d: %v", bucketID, err)
			return nil, err
		}

		value, err := Decrypt(block, s.key)
		if err != nil {
			log.Error().Msgf("Failed to decrypt value for bucket %d: %v", bucketID, err)
			return nil, err
		}
		values[bucketID] = value
		log.Trace().Msgf("Decrypted value for bucket %d: %s", bucketID, value)
	}
	log.Debug().Msgf("Pipelining invalidation and access count updates for %d buckets", len(bucketOffsets))
	invalidateMap := make(map[int]*redis.IntCmd)
	bucketIDs := make([]int, len(bucketOffsets))
	// i := 0
	for bucketID := range bucketOffsets {
		bucketIDs = append(bucketIDs, bucketID)
	}
	metadataMap, err := s.BatchGetAllMetaData(bucketIDs, storageID)
	if err != nil {
		log.Error().Msgf("Failed to get metadata for invalidation: %v", err)
		return nil, err
	}
	for bucketID, offset := range bucketOffsets {
		metadata := metadataMap[bucketID]
		for _, pos := range metadata {
			if pos == offset {
				log.Debug().Msgf("Pipelining HSet to invalidate offset %d in bucket %d", offset, bucketID)
				cmd := pipe.HSet(ctx, strconv.Itoa(-1*bucketID), strconv.Itoa(pos), "__null__")
				invalidateMap[bucketID] = cmd
			}
		}
		log.Debug().Msgf("Pipelining HIncrBy for accessCount in bucket %d", bucketID)
		cmd := pipe.HIncrBy(ctx, strconv.Itoa(-1*bucketID), "accessCount", 1)
		invalidateMap[bucketID] = cmd
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Error().Msgf("Error executing batch read block pipe for invalidation: %v", err)
		return nil, err
	}
	log.Debug().Msg("Finished BatchReadBlock. Read and invalidated.")
	return values, nil
}

// GetBucketsInPaths return all the bucket ids for the passed paths.
func (s *StorageHandler) GetBucketsInPaths(paths []int) (bucketIDs []int, err error) {
	log.Debug().Msgf("Getting buckets in %d paths: %v", len(paths), paths)
	buckets := make(IntSet)
	for i := 0; i < len(paths); i++ {
		leafID := int(math.Pow(2, float64(s.treeHeight-1)) + float64(paths[i]) - 1)
		log.Debug().Msgf("Processing path %d, which maps to leaf ID %d", paths[i], leafID)
		for bucketId := leafID; bucketId > 0; bucketId = bucketId >> s.shift {
			if buckets.Contains(bucketId) {
				log.Debug().Msgf("Bucket %d already added. Breaking path traversal.", bucketId)
				break
			} else {
				buckets.Add(bucketId)
				log.Debug().Msgf("Added new bucket %d to the set.", bucketId)
			}
		}
	}
	bucketIDs = make([]int, len(buckets))

	i := 0

	for key := range buckets {

		bucketIDs[i] = key

		i++
	}
	log.Debug().Msgf("Found %d unique buckets in the given paths: %v", len(bucketIDs), bucketIDs)
	return bucketIDs, nil
}

// It returns valid randomly chosen path and storageID.
func GetRandomPathAndStorageID(treeHeight int, storageCount int) (path int, storageID int) {
	log.Debug().Msg("Getting a random path and storage ID.")
	paths := int(math.Pow(2, float64(treeHeight-1)))
	randomPath := rand.Intn(paths) + 1
	randomStorage := rand.Intn(storageCount)
	log.Debug().Msgf("Generated random path: %d, random storage ID: %d", randomPath, randomStorage)
	return randomPath, randomStorage
}

func (s *StorageHandler) GetRandomStorageID() int {
	log.Debug().Msg("Getting a random storage ID.")
	index := rand.Intn(len(s.storages))
	// i := 0
	for storageID := range s.storages {
		if index == 0 {
			log.Debug().Msgf("Selected random storage ID %d", storageID)
			return storageID
		}
		index--
	}
	return -1
}

func (s *StorageHandler) GetMultipleReverseLexicographicPaths(evictionCount int, count int) (paths []int) {
	log.Debug().Msgf("Getting %d multiple reverse lexicographic paths starting from eviction count %d", count, evictionCount)
	paths = make([]int, count)
	for i := 0; i < count; i++ {
		paths[i] = GetNextReverseLexicographicPath(evictionCount, s.treeHeight)
		log.Debug().Msgf("Path %d: %d", i+1, paths[i])
		evictionCount++
	}
	log.Debug().Msgf("Finished generating %d paths.", len(paths))
	return paths
}

// evictionCount starts from zero and goes forward
func GetNextReverseLexicographicPath(evictionCount int, treeHeight int) (nextPath int) {
	evictionCount = evictionCount % int(math.Pow(2, float64(treeHeight-1)))
	log.Debug().Msgf("Getting next reverse lexicographic path for eviction count %d and tree height %d", evictionCount, treeHeight)
	reverseBinary := utils.BinaryReverse(evictionCount, treeHeight-1)
	nextPath = reverseBinary + 1
	log.Debug().Msgf("Calculated next path: %d (binary reversed from %d)", nextPath, evictionCount)
	return nextPath
}
