package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/dsg-uwaterloo/oblishard/pkg/client"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/tracing"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type readResponse struct {
	block string
	value string
	err   error
}

type writeResponse struct {
	block   string
	success bool
	err     error
}

func asyncRead(tracer trace.Tracer, block string, routerRPCClient client.RouterRPCClient, readResponseChannel chan readResponse) {
	ctx, span := tracer.Start(context.Background(), "client read request")
	value, err := routerRPCClient.Read(ctx, block)
	span.End()
	if err != nil {
		readResponseChannel <- readResponse{block: block, value: "", err: fmt.Errorf("failed to call Read block %s on router; %v", block, err)}
	} else if value == "" {
		readResponseChannel <- readResponse{block: block, value: "", err: fmt.Errorf("block %s does not exist", block)}
	} else {
		readResponseChannel <- readResponse{block: block, value: value, err: nil}
	}
}

func asyncWrite(tracer trace.Tracer, block string, newValue string, routerRPCClient client.RouterRPCClient, writeResponseChannel chan writeResponse) {
	ctx, span := tracer.Start(context.Background(), "client write request")
	value, err := routerRPCClient.Write(ctx, block, newValue)
	span.End()
	if err != nil {
		writeResponseChannel <- writeResponse{block: block, success: false, err: fmt.Errorf("failed to call Write block %s on router; %v", block, err)}
	} else {
		writeResponseChannel <- writeResponse{block: block, success: value, err: nil}
	}
}

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

	readResponseChannel := make(chan readResponse)
	writeResponseChannel := make(chan writeResponse)
	readOperations := 0
	writeOperations := 0

	for _, request := range requests {
		if request.OperationType == client.Read {
			readOperations++
			go asyncRead(tracer, request.Block, routerRPCClient, readResponseChannel)
		} else if request.OperationType == client.Write {
			writeOperations++
			go asyncWrite(tracer, request.Block, request.NewValue, routerRPCClient, writeResponseChannel)
		}
	}
	for i := 0; i <= readOperations+writeOperations; i++ {
		select {
		case readResponse := <-readResponseChannel:
			if readResponse.err != nil {
				log.Error().Msgf(readResponse.err.Error())
			} else {
				log.Debug().Msgf("Sucess in Read of block %s. Got value: %v\n", readResponse.block, readResponse.value)
			}
		case writeResponse := <-writeResponseChannel:
			if writeResponse.err != nil {
				log.Error().Msgf(writeResponse.err.Error())
			} else {
				log.Debug().Msgf("Finished writing block %s. Success: %v\n", writeResponse.block, writeResponse.success)
			}
		}
	}
}
