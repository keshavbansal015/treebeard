package main

import (
	"context"
	"flag"
	"os"
	"path"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	oramnode "github.com/dsg-uwaterloo/oblishard/pkg/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/tracing"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Usage: ./oramnode -oramnodeid=<oramnodeid> -ip=<ip> -rpcport=<rpcport> -replicaid=<replicaid> -raftport=<raftport> -joinaddr=<ip:port> -conf=<configs path> -logpath=<log path>
func main() {
	oramNodeID := flag.Int("oramnodeid", 0, "oramnode id, starting consecutively from zero")
	ip := flag.String("ip", "127.0.0.1", "ip of this replica")
	replicaID := flag.Int("replicaid", 0, "replica id, starting consecutively from zero")
	rpcPort := flag.Int("rpcport", 0, "node rpc port")
	raftPort := flag.Int("raftport", 0, "node raft port")
	joinAddr := flag.String("joinaddr", "", "the address of the initial raft node, which bootstraped the cluster")
	configsPath := flag.String("conf", "../../configs/default", "configs directory path")
	logPath := flag.String("logpath", "", "path to write logs")
	flag.Parse()
	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		os.Exit(1)
	}
	utils.InitLogging(parameters.Log, *logPath)
	if *rpcPort == 0 {
		log.Fatal().Msgf("The rpc port should be provided with the -rpcport flag")
	}
	if *raftPort == 0 {
		log.Fatal().Msgf("The raft port should be provided with the -raftport flag")
	}
	shardNodeEndpoints, err := config.ReadShardNodeEndpoints(path.Join(*configsPath, "shardnode_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := oramnode.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}
	redisEndpoints, err := config.ReadRedisEndpoints(path.Join(*configsPath, "redis_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read redis endpoints from yaml file; %v", err)
	}

	tracingProvider, err := tracing.NewProvider(context.Background(), "oramnode", "localhost:4317", !parameters.Trace)
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	oramnode.StartServer(*oramNodeID, *ip, *rpcPort, *replicaID, *raftPort, *joinAddr, rpcClients, redisEndpoints, parameters)
}
