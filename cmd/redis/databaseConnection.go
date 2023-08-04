package main

import (
	"context"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

type client struct {
	host string
	db   int
	key  []byte
}

type data struct {
	name  string
	value int
}

const (
    Z = 4;
    S = 6;
)


func NewClient(host string, db int, key []byte) *client {
	return &client{
		host: host,
		db:   db,
		key:  key,
	}
}

func (info *client) CloseClient() (err error) {
	client := info.getClient()
	err = client.Close()
	if err != nil {
		return err
	}
	return nil
}

func (info *client) getClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     info.host,
		Password: "",
		DB:       info.db,
	})
}

func (info *client) Set(key string, value string) (err error) {
	client := info.getClient()
	ctx := context.Background()
	err = client.Set(ctx, key, value, 0).Err()
	if err != nil {
	 return err
	}
	return nil
}
   
func (info *client) Get(bucketId int, bit int) (value string, err error) {
	client := info.getClient()
	ctx := context.Background()
	value, err = client.LIndex(ctx, strconv.Itoa(bucketId), int64(bit)).Result()
	if err != nil {
		return "", err
	}
	return value, nil
}

func shuffleArray(arr []int) {
    rand.Seed(time.Now().UnixNano())

    for i := len(arr) - 1; i > 0; i-- {
        j := rand.Intn(i + 1)
        arr[i], arr[j] = arr[j], arr[i]
    }
}

func (info *client) databaseInit(filepath string) (position_map map[string]int, err error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	client := info.getClient()
	ctx := context.Background()
	
	// i keeps track of whether we should load dummies; when i reach Z, add dummies
	i := 0
	bucketCount := 1
	// userID of dummies
	dummyCount := 0
	// initialize map
	position_map = make(map[string]int)
	// initialize value array
	values := make([]string, Z + S)
	metadatas := make([]string, Z + S)
	realIndex := make ([]int, Z + S)
	for k := 0; k < Z + S; k++ {
		// Generate a random number between 0 and 9
		realIndex[k] = k
	}
	shuffleArray(realIndex)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		userID := parts[1]
		value := parts[2]
		value, err = Encrypt(value, info.key)
		if err != nil {
			fmt.Println("Error encrypting data")
			return nil, err
		}
		// add encrypted value to array
		values[realIndex[i]] = value
		// push meta data to array
		metadatas[i] = strconv.Itoa(realIndex[i]) + userID
		// put userId into position map
		position_map[userID] = bucketCount
		i++
		// push dummy values if the current bucket is full
		if i == Z {
			for ; i < Z + S; i++ {
				dummyID := "dummy" + strconv.Itoa(dummyCount)
				dummyString := "b" + strconv.Itoa(bucketCount) + "d" + strconv.Itoa(i)
				dummyString, err = Encrypt(dummyString, info.key)
				if err != nil {
					fmt.Println("Error encrypting data")
					return nil, err
				}
				// push dummy to array
				values[realIndex[i]] = dummyString
				// push meta data of dummies to array
				metadatas[i] = strconv.Itoa(realIndex[i]) + dummyID
				dummyCount++
			}
			// push content of value array and meta data array
			err = info.PushContent(bucketCount, values)
			if err != nil {
				fmt.Println("Error pushing values to db:", err)
				return nil, err
			}
			err = info.PushContent(-1 * bucketCount, metadatas)
			if err != nil {
				fmt.Println("Error pushing metadatas to db:", err)
				return nil, err
			}
			err = client.RPush(ctx, strconv.Itoa(-1 * bucketCount), Z).Err()
			if err != nil {
				fmt.Println("Error pushing dummy meta data start to db:", err)
				return nil, err
			}
			i = 0
			bucketCount++
			// generate new random index for next bucket
			shuffleArray(realIndex)
		}
	}
	return position_map, nil
}

func (info *client) DatabaseClear() (err error) {
	client := info.getClient()
	ctx := context.Background()
	err = client.FlushAll(ctx).Err()
	if err != nil {
		return err
	}
	return nil
}

func (info *client) GetMetadata(bucketId int, bit int) (value string, err error) {
	client := info.getClient()
	ctx := context.Background()
	value, err = client.LIndex(ctx, strconv.Itoa(-1 * bucketId), int64(bit)).Result()
	if err != nil {
		return "", err
	}
	return value, nil
}

func (info *client) PushContent(bucketId int, value []string) (err error) {
	client := info.getClient()
	ctx := context.Background()
	for i := 0; i < len(value); i++ {
		err = client.RPush(ctx, strconv.Itoa(bucketId), value[i]).Err()
		if err != nil {
			return err
		}
	}
	return nil
}


func main() {
	key := []byte("passphrasewhichneedstobe32bytes!")
	info := NewClient("localhost:6379", 1, key)
	path := "./data.txt"

	posmap, err := info.databaseInit(path)
	if err != nil {
		fmt.Println("error initializing database")
	}
	pathId := posmap["user5241976879437760820"]
	fmt.Println(pathId)
	
	vmap, err := info.readPath(pathId, "user5241976879437760820")
	for key, value := range vmap {
		fmt.Printf("Key: %s, Value: %s \n", key, value)
	}
	info.DatabaseClear()
	info.CloseClient()
}
