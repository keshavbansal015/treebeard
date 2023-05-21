package main

import (
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	router "github.com/dsg-uwaterloo/oblishard/pkg/router"
)

func main() {
	shardNodeEndpoints, err := config.ReadEndpoints("../../configs/router_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	shardNodeRPCClients, err := router.StartRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatalf("Failed to create client connections with shard node servers; %v", err)
	}
	router.StartRPCServer(shardNodeRPCClients)
}
