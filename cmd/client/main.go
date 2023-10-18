package main

import (
	"context"
	"flag"

	"github.com/dsg-uwaterloo/oblishard/pkg/client"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/tracing"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
)

// Usage: go run . -logpath=<log path>
func main() {
	logPath := flag.String("logpath", "", "path to write logs")
	flag.Parse()
	utils.InitLogging(true, *logPath)

	routerEndpoints, err := config.ReadRouterEndpoints("../../configs/router_endpoints.yaml")
	if err != nil {
		log.Fatal().Msgf("Cannot read router endpoints from yaml file; %v", err)
	}

	rpcClients, err := client.StartRouterRPCClients(routerEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to start clients; %v", err)
	}

	requests, err := client.ReadTraceFile("../../traces/simple.trace")
	if err != nil {
		log.Fatal().Msgf("Failed to read trace file; %v", err)
	}

	routerRPCClient := rpcClients.GetRandomRouter()

	tracingProvider, err := tracing.NewProvider(context.Background(), "client", "localhost:4317")
	if err != nil {
		log.Fatal().Msgf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatal().Msgf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	tracer := otel.Tracer("")

	for _, request := range requests {
		if request.OperationType == client.Read {
			ctx, span := tracer.Start(context.Background(), "client read request")
			value, err := routerRPCClient.Read(ctx, request.Block)
			span.End()
			if err != nil {
				log.Error().Msgf("Failed to call Read block %s on router; %v", request.Block, err)
				continue
			}
			if value == "" {
				log.Error().Msgf("Block %s does not exist", request.Block)
				continue
			}
			log.Debug().Msgf("Sucess in Read of block %s. Got value: %v\n", request.Block, value)
		} else {
			ctx, span := tracer.Start(context.Background(), "client write request")
			value, err := routerRPCClient.Write(ctx, request.Block, request.NewValue)
			span.End()
			if err != nil {
				log.Error().Msgf("Failed to call Write on router; %v", err)
				continue
			}
			log.Debug().Msgf("Finished writing block %s. Success: %v\n", request.Block, value)
		}
	}
}
