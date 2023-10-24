package main

import (
	"context"
	"flag"
	"path"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	router "github.com/dsg-uwaterloo/oblishard/pkg/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/tracing"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Usage: ./router -routerid=<routerid> -ip=<ip> -port=<port> -conf=<configs path> -logpath=<log path>
func main() {

	routerID := flag.Int("routerid", 0, "router id, starting consecutively from zero")
	ip := flag.String("ip", "", "ip of this replica")
	port := flag.Int("port", 0, "node port")
	configsPath := flag.String("conf", "", "configs directory path")
	logPath := flag.String("logpath", "", "path to write the logs")
	flag.Parse()
	utils.InitLogging(true, *logPath)
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

	tracingProvider, err := tracing.NewProvider(context.Background(), "router", "localhost:4317")
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	router.StartRPCServer(*ip, rpcClients, *routerID, *port)
}
