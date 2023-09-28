package main

import (
	"context"
	"flag"
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	oramnode "github.com/dsg-uwaterloo/oblishard/pkg/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/tracing"
)

// Usage: ./oramnode -oramnodeid=<oramnodeid> -rpcport=<rpcport> -replicaid=<replicaid> -raftport=<raftport> -joinaddr=<ip:port>
func main() {
	oramNodeID := flag.Int("oramnodeid", 0, "oramnode id, starting consecutively from zero")
	replicaID := flag.Int("replicaid", 0, "replica id, starting consecutively from zero")
	rpcPort := flag.Int("rpcport", 0, "node rpc port")
	raftPort := flag.Int("raftport", 0, "node raft port")
	joinAddr := flag.String("joinaddr", "", "the address of the initial raft node, which bootstraped the cluster")
	flag.Parse()
	if *rpcPort == 0 {
		log.Fatalf("The rpc port should be provided with the -rpcport flag")
	}
	if *raftPort == 0 {
		log.Fatalf("The raft port should be provided with the -raftport flag")
	}

	shardNodeEndpoints, err := config.ReadShardNodeEndpoints("../../configs/shardnode_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := oramnode.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatalf("Failed to create client connections with shard node servers; %v", err)
	}

	parameters, err := config.ReadParameters("../../configs/parameters.yaml")
	if err != nil {
		log.Fatalf("Failed to read parameters from yaml file; %v", err)
	}

	// TODO: add a replica id to this
	// TODO: read the exporter url from a config file or sth like that
	tracingProvider, err := tracing.NewProvider(context.Background(), "oramnode", "localhost:4317")
	if err != nil {
		log.Fatalf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatalf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	oramnode.StartServer(*oramNodeID, *rpcPort, *replicaID, *raftPort, *joinAddr, rpcClients, parameters)
}
