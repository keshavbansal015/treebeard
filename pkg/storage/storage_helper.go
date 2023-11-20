package storage

import (
	"context"
	"math"
	"math/rand"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

func getClient(ip string, port int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     ip + ":" + strconv.Itoa(port),
		Password: "",
		DB:       0,
	})
}

func closeClient(client *redis.Client) (err error) {
	err = client.Close()
	if err != nil {
		return err
	}
	return nil
}

func shuffleArray(arr []int) {
	for i := len(arr) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func (s *StorageHandler) databaseInit(redisClient *redis.Client) (err error) {
	for bucketID := 1; bucketID < int(math.Pow(2, float64(s.treeHeight))); bucketID++ {
		values := make([]string, s.Z+s.S)
		metadatas := make([]string, s.Z+s.S)
		realIndex := make([]int, s.Z+s.S)
		for k := 0; k < s.Z+s.S; k++ {
			// Generate a random number between 0 and 9
			realIndex[k] = k
		}
		// userID of dummies
		dummyCount := 0
		// initialize value array
		shuffleArray(realIndex)
		for i := 0; i < s.Z+s.S; i++ {
			dummyID := "dummy" + strconv.Itoa(dummyCount)
			dummyString := "b" + strconv.Itoa(bucketID) + "d" + strconv.Itoa(realIndex[i])
			dummyString, err = Encrypt(dummyString, s.key)
			if err != nil {
				log.Error().Msgf("Error encrypting data")
				return err
			}
			// push dummy to array
			values[realIndex[i]] = dummyString
			// push meta data of dummies to array
			metadatas[i] = strconv.Itoa(realIndex[i]) + dummyID
			dummyCount++
		}
		// push content of value array and meta data array
		err := s.PushDataAndMetadata(bucketID, values, metadatas, redisClient)
		if err != nil {
			log.Error().Msgf("Error pushing values to db: %v", err)
			return err
		}
	}
	return nil
}

func (s *StorageHandler) PushDataAndMetadata(bucketId int, valueData []string, valueMetadata []string, client *redis.Client) (err error) {
	ctx := context.Background()
	kvpMapData := make(map[string]interface{})
	for i := 0; i < len(valueData); i++ {
		kvpMapData[strconv.Itoa(i)] = valueData[i]
	}

	kvpMapMetadata := make(map[string]interface{})
	for i := 0; i < len(valueMetadata); i++ {
		kvpMapMetadata[strconv.Itoa(i)] = valueMetadata[i]
	}
	kvpMapMetadata["nextDummy"] = s.Z
	kvpMapMetadata["accessCount"] = 0

	pipe := client.TxPipeline()
	pipe.HMSet(ctx, strconv.Itoa(bucketId), kvpMapData)
	pipe.HMSet(ctx, strconv.Itoa(-1*bucketId), kvpMapMetadata)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

// return pos + key as one string stored in metadata at bit
func (s *StorageHandler) GetMetadata(bucketId int, bit string, storageID int) (pos int, key string, err error) {
	ctx := context.Background()
	block, err := s.storages[storageID].HGet(ctx, strconv.Itoa(-1*bucketId), bit).Result()
	if err != nil {
		return -1, "", err
	}
	// parse block into pos + key
	index := 0
	for j, char := range block {
		if char < '0' || char > '9' {
			index = j
			break
		}
	}
	pos, err = strconv.Atoi(block[:index])
	if err != nil {
		return -1, "", err
	}
	key = block[index:]
	return pos, key, nil
}

type IntSet map[int]struct{}

func (s IntSet) Add(item int) {
	s[item] = struct{}{}
}

func (s IntSet) Remove(item int) {
	delete(s, item)
}

func (s IntSet) Contains(item int) bool {
	_, found := s[item]
	return found
}
