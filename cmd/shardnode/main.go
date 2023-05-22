package main

import (
	"log"
	"os"
	"strconv"

	shardnode "github.com/dsg-uwaterloo/oblishard/pkg/shardnode"
)

// Usage: ./shardnode <id> <port>
func main() {
	shardNodeID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("The shardNodeId should be provided as a CLI argument; %v", err)
	}
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("The port should be provided as a CLI argument; %v", err)
	}
	shardnode.StartRPCServer(shardNodeID, port)
}
