package main

import (
	"log"
	"os"
	"strconv"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	router "github.com/dsg-uwaterloo/oblishard/pkg/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
)

// Usage: ./router <id> <port>
func main() {
	routerID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("The routerId should be provided as a CLI argument; %v", err)
	}
	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("The port should be provided as a CLI argument; %v", err)
	}

	shardNodeEndpoints, err := config.ReadEndpoints("../../configs/shardnode_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := rpc.StartRPCClients(shardNodeEndpoints, &router.ShardNodeClientFactory{})
	if err != nil {
		log.Fatalf("Failed to create client connections with shard node servers; %v", err)
	}

	shardNodeRPCClients := router.ConvertRPCClientInterfaces(rpcClients)

	router.StartRPCServer(shardNodeRPCClients, routerID, port)
}
