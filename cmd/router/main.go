package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/profile"
	router "github.com/keshavbansal015/treebeard/pkg/router"
	"github.com/keshavbansal015/treebeard/pkg/tracing"
	"github.com/keshavbansal015/treebeard/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Usage: ./router -h
func main() {
	log.Debug().Msg("Starting router main function.")
	routerID := flag.Int("routerid", 0, "router id, starting consecutively from zero")
	ip := flag.String("ip", "127.0.0.1", "ip of this replica")
	port := flag.Int("port", 0, "node port")
	configsPath := flag.String("conf", "../../configs/default", "configs directory path")
	logPath := flag.String("logpath", "", "path to write the logs")
	flag.Parse()
	log.Debug().Msgf("Parsed command line flags: routerID=%d, ip=%s, port=%d, configsPath=%s, logPath=%s", *routerID, *ip, *port, *configsPath, *logPath)

	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		log.Debug().Msgf("Failed to read parameters.yaml: %v", err)
		os.Exit(1)
	}
	utils.InitLogging(parameters.Log, *logPath)
	log.Debug().Msg("Logging initialized.")

	if *port == 0 {
		log.Fatal().Msgf("The port should be provided with the -port flag")
	}

	shardNodeEndpoints, err := config.ReadShardNodeEndpoints(path.Join(*configsPath, "shardnode_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	log.Debug().Msg("Successfully read shard node endpoints.")

	rpcClients, err := router.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}
	log.Debug().Msg("Started RPC clients for shard nodes.")

	tracingProvider, err := tracing.NewProvider(context.Background(), "router", "localhost:4317", !parameters.Trace)
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
		fileName := fmt.Sprintf("router_cpu_%d.prof", *routerID)
		cpuProfile := profile.NewCPUProfile(fileName)
		cpuProfile.Start()
		log.Debug().Msgf("CPU profiling started, saving to file: %s", fileName)
		defer cpuProfile.Stop()
	}

	log.Debug().Msg("Starting RPC server.")
	router.StartRPCServer(*ip, rpcClients, *routerID, *port, parameters)
	log.Debug().Msg("RPC server started successfully.")
}
