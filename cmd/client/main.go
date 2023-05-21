package main

import (
	"fmt"
	"log"

	client "github.com/dsg-uwaterloo/oblishard/pkg/client"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
)

func main() {
	routerEndpoints, err := config.ReadEndpoints("../../configs/router_endpoints.yaml")
	if err != nil {
		log.Fatalf("Cannot read router endpoints from yaml file; %v", err)
	}

	rpcClients, err := client.StartRPCClients(routerEndpoints)
	if err != nil {
		log.Fatalf("Failed to start clients; %v", err)
	}
	for _, rpcClient := range rpcClients {
		defer rpcClient.Conn.Close()
	}

	testRPCClient := rpcClients[0]
	value, err := testRPCClient.Read("a")
	if err != nil {
		log.Printf("Failed to call Read on router; %v", err)
		return
	}
	fmt.Printf("Sucess in Read. Got value: %v", value)
}
