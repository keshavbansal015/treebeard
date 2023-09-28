package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/client"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/tracing"
	"go.opentelemetry.io/otel"
)

func main() {
	routerEndpoints, err := config.ReadRouterEndpoints("../../configs/router_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read router endpoints from yaml file; %v", err)
	}

	rpcClients, err := client.StartRouterRPCClients(routerEndpoints)
	if err != nil {
		log.Fatalf("Failed to start clients; %v", err)
	}

	requests, err := client.ReadTraceFile("../../traces/simple.trace")
	if err != nil {
		log.Fatalf("Failed to read trace file; %v", err)
	}

	routerRPCClient := rpcClients.GetRandomRouter()

	tracingProvider, err := tracing.NewProvider(context.Background(), "client", "localhost:4317")
	if err != nil {
		log.Fatalf("Failed to create tracing provider; %v", err)
	}
	stopTracingProvider, err := tracingProvider.RegisterAsGlobal()
	if err != nil {
		log.Fatalf("Failed to register tracing provider; %v", err)
	}
	defer stopTracingProvider(context.Background())

	tracer := otel.Tracer("")

	for _, request := range requests {
		if request.OperationType == client.Read {
			ctx, span := tracer.Start(context.Background(), "client read request")
			value, err := routerRPCClient.Read(ctx, request.Block)
			span.End()
			if err != nil {
				log.Printf("Failed to call Read on router; %v", err)
				return
			}
			fmt.Printf("Sucess in Read. Got value: %v\n", value)
		} else {
			ctx, span := tracer.Start(context.Background(), "client write request")
			value, err := routerRPCClient.Write(ctx, request.Block, request.NewValue)
			span.End()
			if err != nil {
				log.Printf("Failed to call Write on router; %v", err)
				return
			}
			fmt.Printf("Sucess in Write. Success: %v\n", value)
		}
	}
}
