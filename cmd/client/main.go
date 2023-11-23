package main

import (
	"context"
	"flag"
	"fmt"
	"path"
	"time"

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

// TODO: Add client struct that contains the routerRPCClient and the rateLimit
func asyncRead(ratelimit *client.RateLimit, tracer trace.Tracer, block string, routerRPCClient client.RouterRPCClient, readResponseChannel chan readResponse) {
	ratelimit.Wait()
	ctx, span := tracer.Start(context.Background(), "client read request")
	value, err := routerRPCClient.Read(ctx, block)
	ratelimit.AddToken()
	span.End()
	if err != nil {
		readResponseChannel <- readResponse{block: block, value: "", err: fmt.Errorf("failed to call Read block %s on router; %v", block, err)}
	} else if value == "" {
		readResponseChannel <- readResponse{block: block, value: "", err: nil}
	} else {
		readResponseChannel <- readResponse{block: block, value: value, err: nil}
	}
}

func asyncWrite(ratelimit *client.RateLimit, tracer trace.Tracer, block string, newValue string, routerRPCClient client.RouterRPCClient, writeResponseChannel chan writeResponse) {
	ratelimit.Wait()
	ctx, span := tracer.Start(context.Background(), "client write request")
	value, err := routerRPCClient.Write(ctx, block, newValue)
	ratelimit.AddToken()
	span.End()
	if err != nil {
		writeResponseChannel <- writeResponse{block: block, success: false, err: fmt.Errorf("failed to call Write block %s on router; %v", block, err)}
	} else {
		writeResponseChannel <- writeResponse{block: block, success: value, err: nil}
	}
}

// Usage: go run . -logpath=<log path> -conf=<configs path>
func main() {
	logPath := flag.String("logpath", "", "path to write logs")
	configsPath := flag.String("conf", "../../configs/default", "configs directory path")
	flag.Parse()
	utils.InitLogging(false, *logPath)

	routerEndpoints, err := config.ReadRouterEndpoints(path.Join(*configsPath, "router_endpoints.yaml"))
	if err != nil {
		log.Fatal().Msgf("Cannot read router endpoints from yaml file; %v", err)
	}

	rpcClients, err := client.StartRouterRPCClients(routerEndpoints)
	if err != nil {
		log.Fatal().Msgf("Failed to start clients; %v", err)
	}

	requests, err := client.ReadTraceFile(path.Join(*configsPath, "simple.trace"))
	if err != nil {
		log.Fatal().Msgf("Failed to read trace file; %v", err)
	}

	parameters, err := config.ReadParameters(path.Join(*configsPath, "parameters.yaml"))
	if err != nil {
		log.Fatal().Msgf("Failed to read parameters from yaml file; %v", err)
	}

	routerRPCClient := rpcClients.GetRandomRouter()

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

	readResponseChannel := make(chan readResponse)
	writeResponseChannel := make(chan writeResponse)
	readOperations := 0
	writeOperations := 0
	rateLimit := client.NewRateLimit(parameters.MaxRequests)
	rateLimit.Start()
	startTime := time.Now()
	for _, request := range requests {
		if request.OperationType == client.Read {
			readOperations++
			go asyncRead(rateLimit, tracer, request.Block, routerRPCClient, readResponseChannel)
		} else if request.OperationType == client.Write {
			writeOperations++
			go asyncWrite(rateLimit, tracer, request.Block, request.NewValue, routerRPCClient, writeResponseChannel)
		}
	}
	for i := 0; i < readOperations+writeOperations; i++ {
		select {
		case readResponse := <-readResponseChannel:
			if readResponse.err != nil {
				fmt.Println(readResponse.err.Error())
				log.Error().Msgf(readResponse.err.Error())
			} else {
				log.Debug().Msgf("Sucess in Read of block %s. Got value: %v\n", readResponse.block, readResponse.value)
			}
		case writeResponse := <-writeResponseChannel:
			if writeResponse.err != nil {
				fmt.Println(writeResponse.err.Error())
				log.Error().Msgf(writeResponse.err.Error())
			} else {
				log.Debug().Msgf("Finished writing block %s. Success: %v\n", writeResponse.block, writeResponse.success)
			}
		}
	}
	elapsed := time.Since(startTime)
	fmt.Printf("Throughput: %f", float64(readOperations+writeOperations)/elapsed.Seconds())
}
