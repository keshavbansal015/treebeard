package main

import (
	"context"
	// "encoding/json"
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type client struct {
	host string
	db   int
}

type data struct {
	name  string
	value int
}

func NewClient(host string, db int) *client {
	return &client{
		host: host,
		db:   db,
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

func (info *client) databaseInit(filepath string) (err error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	client := info.getClient()
	ctx := context.Background()

	i := 0
	bucketCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		err = client.RPush(ctx, strconv.Itoa(bucketCount), line).Err()
		if err != nil {
			return err
		}
		i++
		if i == 4 {
			for ; i <= 9; i++ {
				dummyString := "b" + strconv.Itoa(bucketCount) + "d" + strconv.Itoa(i)
				err = client.RPush(ctx, strconv.Itoa(bucketCount), dummyString).Err()
				if err != nil {
					return err
				}
			}
			i = 0
			bucketCount++
		}
	}
	return nil
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

func (info *client) GetValueByIndex(bucketId int, index int64) (value string, err error) {
	client := info.getClient()
	ctx := context.Background()
	value, err = client.LIndex(ctx, strconv.Itoa(bucketId), index).Result()
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
	info := NewClient("localhost:6379", 1)
	path := "./data.txt"

	err := info.databaseInit(path)
	if err != nil {
		fmt.Println("error initializing database")
	}
	value, err := info.GetValueByIndex(0, 0)
	fmt.Println(value)
	value, err = info.GetValueByIndex(1, 6)
	fmt.Println(value)

	info.DatabaseClear()
	info.CloseClient()
}
