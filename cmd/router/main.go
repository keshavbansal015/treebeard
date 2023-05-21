package main

import (
	"log"

	router "github.com/dsg-uwaterloo/oblishard/pkg/router"
)

func main() {
	shardNodeRPCClients, err := router.StartRPCClients()
	if err != nil {
		log.Fatalf("Failed to create client connections with shard node servers; %v", err)
	}
	router.StartRPCServer(shardNodeRPCClients)
}
