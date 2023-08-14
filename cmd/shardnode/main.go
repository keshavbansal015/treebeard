package main

import (
	"flag"
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	shardnode "github.com/dsg-uwaterloo/oblishard/pkg/shardnode"
)

// Usage: ./shardnode -shardnodeid=<shardnodeid> -rpcport=<rpcport> -replicaid=<replicaid> -raftport=<raftport> -joinaddr=<ip:port>
func main() {
	shardNodeID := flag.Int("shardnodeid", 0, "shardnode id, starting consecutively from zero")
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

	oramNodeEndpoints, err := config.ReadOramNodeEndpoints("../../configs/oramnode_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := shardnode.StartOramNodeRPCClients(oramNodeEndpoints)
	if err != nil {
		log.Fatalf("Failed to create client connections with oarm node servers; %v", err)
	}

	parameters, err := config.ReadParameters("../../configs/parameters.yaml")
	if err != nil {
		log.Fatalf("Failed to read parameters from yaml file; %v", err)
	}

	shardnode.StartServer(*shardNodeID, *rpcPort, *replicaID, *raftPort, *joinAddr, rpcClients, parameters)
}
