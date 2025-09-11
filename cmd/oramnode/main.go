package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/keshavbansal015/treebeard/pkg/config"
	oramnode "github.com/keshavbansal015/treebeard/pkg/oramnode"
	"github.com/keshavbansal015/treebeard/pkg/profile"
	"github.com/keshavbansal015/treebeard/pkg/tracing"
	"github.com/keshavbansal015/treebeard/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Usage: ./oramnode -h
func main() {
	log.Debug().Msg("Starting oramnode main function.")
	oramNodeID := flag.Int("oramnodeid", 0, "oramnode id, starting consecutively from zero")
	bindIP := flag.String("bindip", "127.0.0.1", "ip of this replica to bind to")
	advIP := flag.String("advip", "127.0.0.1", "ip of this replica to advertise")
	replicaID := flag.Int("replicaid", 0, "replica id, starting consecutively from zero")
	rpcPort := flag.Int("rpcport", 0, "node rpc port")
	raftPort := flag.Int("raftport", 0, "node raft port")
	joinAddr := flag.String("joinaddr", "", "the address of the initial raft node, which bootstraped the cluster")
	configsPath := flag.String("conf", "../../configs/default", "configs directory path")
	logPath := flag.String("logpath", "", "path to write logs")
	flag.Parse()
	log.Debug().Msgf("Parsed command line flags: oramNodeID=%d, bindIP=%s, advIP=%s, replicaID=%d, rpcPort=%d, raftPort=%d, joinAddr=%s, configsPath=%s, logPath=%s", *oramNodeID, *bindIP, *advIP, *replicaID, *rpcPort, *raftPort, *joinAddr, *configsPath, *logPath)

	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		log.Debug().Msgf("Failed to read parameters.yaml: %v", err)
		os.Exit(1)
	}
	utils.InitLogging(parameters.Log, *logPath)
	log.Debug().Msg("Logging initialized.")

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
	log.Debug().Msg("Successfully read shard node endpoints.")

	rpcClients, err := oramnode.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}
	log.Debug().Msg("Started RPC clients for shard nodes.")

	redisEndpoints, err := config.ReadRedisEndpoints(path.Join(*configsPath, "redis_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read redis endpoints from yaml file; %v", err)
	}
	log.Debug().Msg("Successfully read redis endpoints.")

	tracingProvider, err := tracing.NewProvider(context.Background(), "oramnode", "localhost:4317", !parameters.Trace)
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	log.Debug().Msg("Created tracing provider.")

	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	log.Debug().Msg("Registered tracing provider as global.")
	defer stopTracingProvider(context.Background())

	if parameters.Profile {
		fileName := fmt.Sprintf("oramnode_cpu_%d_%d.prof", *oramNodeID, *replicaID)
		cpuProfile := profile.NewCPUProfile(fileName)
		cpuProfile.Start()
		log.Debug().Msgf("CPU profiling started, saving to file: %s", fileName)
		defer cpuProfile.Stop()
	}

	log.Debug().Msg("Starting ORAM server.")
	oramnode.StartServer(*oramNodeID, *bindIP, *advIP, *rpcPort, *replicaID, *raftPort, *joinAddr, rpcClients, redisEndpoints, parameters)
	log.Debug().Msg("ORAM server started successfully.")
}
