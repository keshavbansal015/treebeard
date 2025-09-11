package main

import (
	"context"
	"flag"
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
	log.Debug().Msg("Starting main function.")
	logPath := flag.String("logpath", "", "path to write logs")
	configsPath := flag.String("conf", "../../configs/default", "configs directory path")
	duration := flag.Int("duration", 10, "duration of the experiment in seconds")
	outputFilePath := flag.String("output", "", "output file path")
	flag.Parse()
	log.Debug().Msgf("Parsed command line flags: logpath=%s, configsPath=%s, duration=%d, outputFilePath=%s", *logPath, *configsPath, *duration, *outputFilePath)

	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		log.Debug().Msgf("Failed to read parameters.yaml: %v", err)
		os.Exit(1)
	}
	log.Debug().Msg("Successfully read parameters from parameters.yaml.")

	utils.InitLogging(parameters.Log, *logPath)
	log.Debug().Msg("Logging initialized.")

	routerEndpoints, err := config.ReadRouterEndpoints(path.Join(*configsPath, "router_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read router endpoints from yaml file; %v", err)
	}
	log.Debug().Msg("Successfully read router endpoints.")

	redisEndpoints, err := config.ReadRedisEndpoints(path.Join(*configsPath, "redis_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read redis endpoints from yaml file; %v", err)
	}
	log.Debug().Msg("Successfully read redis endpoints.")

	rpcClients, err := client.StartRouterRPCClients(routerEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to start clients; %v", err)
	}
	log.Debug().Msg("Started RPC clients.")

	requests, err := client.ReadTraceFile(path.Join(*configsPath, "trace.txt"), parameters.BlockSize)
	if err != nil {
		log.Fatal().Msgf("Failed to read trace file; %v", err)
	}
	log.Debug().Msgf("Read %d requests from trace file.", len(requests))

	tracingProvider, err := tracing.NewProvider(context.Background(), "client", "localhost:4317", !parameters.Trace)
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

	tracer := otel.Tracer("")
	log.Debug().Msg("Tracer initialized.")

	c := client.NewClient(client.NewRateLimit(parameters.MaxRequests), tracer, rpcClients, requests)
	log.Debug().Msg("New client created.")

	err = c.WaitForStorageToBeReady(redisEndpoints, parameters)
	if err != nil {
		log.Fatal().Msgf("Failed to check if storages are ready; %v", err)
	}
	log.Debug().Msg("Storage is ready.")

	readResponseChannel := make(chan client.ReadResponse)
	writeResponseChannel := make(chan client.WriteResponse)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*duration)*time.Second)
	defer cancel()
	log.Debug().Msgf("Experiment context created with a timeout of %d seconds.", *duration)

	go c.SendRequestsForever(ctx, readResponseChannel, writeResponseChannel)
	log.Debug().Msg("Started goroutine for sending requests.")

	responseCounts := c.GetResponsesForever(ctx, readResponseChannel, writeResponseChannel)
	log.Debug().Msg("Got responses forever.")

	err = client.WriteOutputToFile(*outputFilePath, responseCounts)
	if err != nil {
		log.Fatal().Msgf("Failed to write output to file; %v", err)
	}
	log.Debug().Msgf("Wrote output to file %s.", *outputFilePath)
	log.Debug().Msg("Main function finished.")
}
