package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal01515/treebeard/pkg/profile"
	router "github.com/keshavbansal01515/treebeard/pkg/router"
	"github.com/keshavbansal01515/treebeard/pkg/tracing"
	"github.com/keshavbansal01515/treebeard/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Usage: ./router -h
func main() {

	routerID := flag.Int("routerid", 0, "router id, starting consecutively from zero")
	ip := flag.String("ip", "127.0.0.1", "ip of this replica")
	port := flag.Int("port", 0, "node port")
	configsPath := flag.String("conf", "../../configs/default", "configs directory path")
	logPath := flag.String("logpath", "", "path to write the logs")
	flag.Parse()
	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		os.Exit(1)
	}
	utils.InitLogging(parameters.Log, *logPath)
	if *port == 0 {
		log.Fatal().Msgf("The port should be provided with the -port flag")
	}

	shardNodeEndpoints, err := config.ReadShardNodeEndpoints(path.Join(*configsPath, "shardnode_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read shard node endpoints from yaml file; %v", err)
	}
	rpcClients, err := router.StartShardNodeRPCClients(shardNodeEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to create client connections with shard node servers; %v", err)
	}

	tracingProvider, err := tracing.NewProvider(context.Background(), "router", "localhost:4317", !parameters.Trace)
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	if parameters.Profile {
		fileName := fmt.Sprintf("router_cpu_%d.prof", *routerID)
		cpuProfile := profile.NewCPUProfile(fileName)
		cpuProfile.Start()
		defer cpuProfile.Stop()
	}

	router.StartRPCServer(*ip, rpcClients, *routerID, *port, parameters)
}
