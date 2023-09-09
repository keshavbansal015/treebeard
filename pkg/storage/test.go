package storage

import (
	"fmt"
)

func main() {
	key := []byte("passphrasewhichneedstobe32bytes!")
	info := NewStorageHandler("localhost:6379", 1, key)
	path := "./data.txt"

	posmap, err := info.databaseInit(path)
	if err != nil {
		fmt.Println("error initializing database")
	}
	pathId := posmap["user5241976879437760820"]
	fmt.Println(pathId)
	blocks := []string{"user5241976879437760820"}
	offset, _, _, err:= info.GetBlockOffset(pathId, 0, blocks)
	val, err := info.ReadBlock(pathId, 0, offset)
	if err == nil {
		fmt.Println(val)
	}
	blocks = []string{"user3861369316569033754"}
	offset, _, _, err = info.GetBlockOffset(pathId, 0, blocks)
	val, err = info.ReadBlock(pathId, 0, offset)
	if err == nil {
		fmt.Println(val)
	}
	readBucketBlocks := map[string]string {
		"userR1" : "read",
	}
	shardNodeBlocks := map[string]string {
		"userS1" : "shard",
		"userS2" : "shard",
	}
	writtenBlocks, err := info.WriteBucket(pathId, 0, readBucketBlocks, shardNodeBlocks, true)
	if err == nil {
		for k, v := range writtenBlocks {
			fmt.Println(k, v)
		}
	}
	rBlocks, err := info.ReadBucket(pathId, 0)
	if err == nil {
		for k, v := range rBlocks {
			fmt.Println(k, v)
		}
	}
	info.DatabaseClear()
	info.CloseClient()
}
