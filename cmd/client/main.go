package main

import (
	"fmt"
	"log"

	"github.com/dsg-uwaterloo/oblishard/pkg/client"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
)

func main() {
	routerEndpoints, err := config.ReadEndpoints("../../configs/router_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read router endpoints from yaml file; %v", err)
	}

	rpcClients, err := rpc.StartRPCClients(routerEndpoints, &client.RouterClientFactory{})
	if err != nil {
		log.Fatalf("Failed to start clients; %v", err)
	}
	routerRPCClients := client.ConvertRPCClientInterfaces(rpcClients)

	testRouterRPCClient := routerRPCClients[0]
	value, err := testRouterRPCClient.Read("a")
	if err != nil {
		log.Printf("Failed to call Read on router; %v", err)
		return
	}
	fmt.Printf("Sucess in Read. Got value: %v", value)
}
