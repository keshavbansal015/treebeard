package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/keshavbansal015/treebeard/pkg/client"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/tracing"
	"github.com/keshavbansal015/treebeard/pkg/utils"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
)

// Usage: ./client -h
func main() {
	logPath := flag.String("logpath", "", "path to write logs")
	configsPath := flag.String("conf", "../../configs/default", "configs directory path")
	duration := flag.Int("duration", 10, "duration of the experiment in seconds")
	outputFilePath := flag.String("output", "", "output file path")
	flag.Parse()
	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		os.Exit(1)
	}

	utils.InitLogging(parameters.Log, *logPath)

	routerEndpoints, err := config.ReadRouterEndpoints(path.Join(*configsPath, "router_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read router endpoints from yaml file; %v", err)
	}

	redisEndpoints, err := config.ReadRedisEndpoints(path.Join(*configsPath, "redis_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read redis endpoints from yaml file; %v", err)
	}

	rpcClients, err := client.StartRouterRPCClients(routerEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to start clients; %v", err)
	}

	requests, err := client.ReadTraceFile(path.Join(*configsPath, "trace.txt"), parameters.BlockSize)
	if err != nil {
		log.Fatal().Msgf("Failed to read trace file; %v", err)
	}

	tracingProvider, err := tracing.NewProvider(context.Background(), "client", "localhost:4317", !parameters.Trace)
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	tracer := otel.Tracer("")

	c := client.NewClient(client.NewRateLimit(parameters.MaxRequests), tracer, rpcClients, requests)
	err = c.WaitForStorageToBeReady(redisEndpoints, parameters)
	if err != nil {
		log.Fatal().Msgf("Failed to check if storages are ready; %v", err)
	}
	fmt.Println("Starting experiment")

	readResponseChannel := make(chan client.ReadResponse)
	writeResponseChannel := make(chan client.WriteResponse)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*duration)*time.Second)
	defer cancel()

	go c.SendRequestsForever(ctx, readResponseChannel, writeResponseChannel)
	responseCounts := c.GetResponsesForever(ctx, readResponseChannel, writeResponseChannel)
	err = client.WriteOutputToFile(*outputFilePath, responseCounts)
	if err != nil {
		log.Fatal().Msgf("Failed to write output to file; %v", err)
	}
}
