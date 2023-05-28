package utils

import "hash/fnv"

func Hash(s string) uint32 { //TODO: optimize for faster hashing
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
