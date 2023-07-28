package main

import (
	"fmt"
	"strconv"
)

// need to implement incrementation of dummyIndex after read
func (info *client) getDummyObject(pathId int) (key string, value string, err error) {
	key, err = info.GetMetadata(pathId, 10)
	if err != nil {
		fmt.Println("error fetching dummy index metadata")
		return "", "", err
	}
	dummyIndex, err := strconv.Atoi(key)
	if err != nil {
		fmt.Println("Unable to convert dummyIndex to int; something went wrong with indexing")
		return "", "", err
	}
	key, err = info.GetMetadata(pathId, dummyIndex)
	if err != nil {
		fmt.Println("error fetching metadata")
		return "", "", err
	}
	value, err = info.Get(key)
	if err != nil {
		fmt.Println(key)
		fmt.Println("error fetching dummy data")
		return "", "", err
	}
	value, err = Decrypt(value, info.key)
	return key, value, nil
}

// blockIndex is the key in our case
func (info *client) readPath(pathId int, blockIndex string) (map[string]string, error) {
	found := false
	value_map := make(map[string]string)
	for posId := pathId; posId > 0; posId = (posId - 1) >> 1 {
		if found {
			key, value, err := info.getDummyObject(posId)
			if err != nil {
				fmt.Println("error fetching dummy data in readPath")
				return nil, err
			}
			value_map[key] = value
		} else {
			for i := 0; i < 4; i++ {
				key, err := info.GetMetadata(posId, i)
				if err != nil {
					fmt.Println("error fetching metadata")
					return nil, err
				}
				if key == blockIndex {
					found = true
					value, err := info.Get(key)
					if err != nil {
						fmt.Println("error fetching data")
						return nil, err
					}
					value, err = Decrypt(value, info.key)
					value_map[key] = value
				}
			}
			if !found {
				key, value, err := info.getDummyObject(posId)
				if err != nil {
					fmt.Println("error fetching dummy data in readPath")
					return nil, err
				}
				value_map[key] = value
			}
		}
	}
	return value_map, nil
}
