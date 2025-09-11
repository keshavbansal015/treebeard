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
	log.Debug().Msg("Starting shardnode main function.")
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
	log.Debug().Msgf("Parsed command line flags: shardNodeID=%d, bindIP=%s, advIP=%s, replicaID=%d, rpcPort=%d, raftPort=%d, joinAddr=%s, configsPath=%s, logPath=%s", *shardNodeID, *bindIP, *advIP, *replicaID, *rpcPort, *raftPort, *joinAddr, *configsPath, *logPath)

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
	log.Debug().Msg("RPC and Raft ports are valid.")

	oramNodeEndpoints, err := config.ReadOramNodeEndpoints(path.Join(*configsPath, "oramnode_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read oram node endpoints from yaml file; %v", err)
	}
	log.Debug().Msg("Successfully read ORAM node endpoints.")

	redisEndpoints, err := config.ReadRedisEndpoints(path.Join(*configsPath, "redis_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read redis endpoints from yaml file; %v", err)
	}
	log.Debug().Msg("Successfully read redis endpoints.")

	rpcClients, err := shardnode.StartOramNodeRPCClients(oramNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with oarm node servers; %v", err)
	}
	log.Debug().Msg("Started RPC clients for ORAM nodes.")

	tracingProvider, err := tracing.NewProvider(context.Background(), "shardnode", "localhost:4317", !parameters.Trace)
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
		fileName := fmt.Sprintf("shardnode_cpu_%d_%d.prof", *shardNodeID, *replicaID)
		cpuProfile := profile.NewCPUProfile(fileName)
		cpuProfile.Start()
		log.Debug().Msgf("CPU profiling started, saving to file: %s", fileName)
		defer cpuProfile.Stop()
	}

	log.Debug().Msg("Starting Shard Node server.")
	shardnode.StartServer(*shardNodeID, *bindIP, *advIP, *rpcPort, *replicaID, *raftPort, *joinAddr, rpcClients, parameters, redisEndpoints, *configsPath)
	log.Debug().Msg("Shard Node server started successfully.")
}
