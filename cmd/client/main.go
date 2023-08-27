package main

import (
	"fmt"
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/client"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
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
	for _, request := range requests {
		if request.OperationType == client.Read {
			value, err := routerRPCClient.Read(request.Block)
			if err != nil {
				log.Printf("Failed to call Read on router; %v", err)
				return
			}
			fmt.Printf("Sucess in Read. Got value: %v\n", value)
		} else {
			value, err := routerRPCClient.Write(request.Block, request.NewValue)
			if err != nil {
				log.Printf("Failed to call Write on router; %v", err)
				return
			}
			fmt.Printf("Sucess in Write. Success: %v\n", value)
		}
	}
}
