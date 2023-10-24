package main

import (
	"context"
	"flag"
	"path"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	shardnode "github.com/dsg-uwaterloo/oblishard/pkg/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/tracing"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Usage: ./shardnode -shardnodeid=<shardnodeid> -ip=<ip> -rpcport=<rpcport> -replicaid=<replicaid> -raftport=<raftport> -raftdir=<raftdir> -joinaddr=<ip:port> -conf=<configs path> -logpath=<log path>
func main() {
	shardNodeID := flag.Int("shardnodeid", 0, "shardnode id, starting consecutively from zero")
	ip := flag.String("ip", "", "ip of this replica")
	replicaID := flag.Int("replicaid", 0, "replica id, starting consecutively from zero")
	rpcPort := flag.Int("rpcport", 0, "node rpc port")
	raftPort := flag.Int("raftport", 0, "node raft port")
	raftDir := flag.String("raftdir", "", "the address of the raft snapshot directory")
	joinAddr := flag.String("joinaddr", "", "the address of the initial raft node, which bootstraped the cluster")
	configsPath := flag.String("conf", "", "configs directory path")
	logPath := flag.String("logpath", "", "path to write logs")
	flag.Parse()

	utils.InitLogging(true, *logPath)
	if *rpcPort == 0 {
		log.Fatal().Msgf("The rpc port should be provided with the -rpcport flag")
	}
	if *raftPort == 0 {
		log.Fatal().Msgf("The raft port should be provided with the -raftport flag")
	}

	oramNodeEndpoints, err := config.ReadOramNodeEndpoints(path.Join(*configsPath, "oramnode_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := shardnode.StartOramNodeRPCClients(oramNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with oarm node servers; %v", err)
	}

	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		log.Fatal().Msgf("Failed to read parameters from yaml file; %v", err)
	}

	tracingProvider, err := tracing.NewProvider(context.Background(), "shardnode", "localhost:4317")
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	shardnode.StartServer(*shardNodeID, *ip, *rpcPort, *replicaID, *raftPort, *raftDir, *joinAddr, rpcClients, parameters, *configsPath)
}
