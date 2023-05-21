package main

import (
	"log"
	"os"
	"strconv"

	shardnode "github.com/dsg-uwaterloo/oblishard/pkg/shardnode"
)

func main() {
	shardNodeID, err := strconv.Atoi(os.Args[1]) //TODO ensure that the ids are unique
	if err != nil {
		log.Fatalf("The shardNodeId should be provided as a CLI argument; %v", err)
	}
	shardnode.StartRPCServer(shardNodeID)
}
