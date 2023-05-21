package main

import (
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	router "github.com/dsg-uwaterloo/oblishard/pkg/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
)

func main() {
	shardNodeEndpoints, err := config.ReadEndpoints("../../configs/shardnode_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := rpc.StartRPCClients(shardNodeEndpoints, &router.ShardNodeClientFactory{})
	if err != nil {
		log.Fatalf("Failed to create client connections with shard node servers; %v", err)
	}

	shardNodeRPCClients := router.ConvertRPCClientInterfaces(rpcClients)

	router.StartRPCServer(shardNodeRPCClients)
}
