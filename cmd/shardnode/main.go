package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/profile"
	shardnode "github.com/keshavbansal015/treebeard/pkg/shardnode"
	"github.com/keshavbansal015/treebeard/pkg/tracing"
	"github.com/keshavbansal015/treebeard/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Usage: ./shardnode -h
func main() {
	shardNodeID := flag.Int("shardnodeid", 0, "shardnode id, starting consecutively from zero")
	bindIP := flag.String("bindip", "127.0.0.1", "bind ip of this replica")
	advIP := flag.String("advip", "127.0.0.1", "advertise ip of this replica")
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

	oramNodeEndpoints, err := config.ReadOramNodeEndpoints(path.Join(*configsPath, "oramnode_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	redisEndpoints, err := config.ReadRedisEndpoints(path.Join(*configsPath, "redis_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read redis endpoints from yaml file; %v", err)
	}

	rpcClients, err := shardnode.StartOramNodeRPCClients(oramNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with oarm node servers; %v", err)
	}

	tracingProvider, err := tracing.NewProvider(context.Background(), "shardnode", "localhost:4317", !parameters.Trace)
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	if parameters.Profile {
		fileName := fmt.Sprintf("shardnode_cpu_%d_%d.prof", *shardNodeID, *replicaID)
		cpuProfile := profile.NewCPUProfile(fileName)
		cpuProfile.Start()
		defer cpuProfile.Stop()
	}

	shardnode.StartServer(*shardNodeID, *bindIP, *advIP, *rpcPort, *replicaID, *raftPort, *joinAddr, rpcClients, parameters, redisEndpoints, *configsPath)
}
