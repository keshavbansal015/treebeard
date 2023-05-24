package main

import (
	"flag"
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	router "github.com/dsg-uwaterloo/oblishard/pkg/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
)

// Usage: ./router -routerid=<routerid> -port=<port>
func main() {
	routerID := flag.Int("routerid", 0, "router id, starting consecutively from zero")
	port := flag.Int("port", 0, "node port")
	flag.Parse()
	if *port == 0 {
		log.Fatalf("The port should be provided with the -port flag")
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

	router.StartRPCServer(shardNodeRPCClients, *routerID, *port)
}
