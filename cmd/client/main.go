package main

import (
	"fmt"
	"log"

	client "github.com/dsg-uwaterloo/oblishard/pkg/client"
)

func main() {
	rpcClient, err := client.StartRPCClient()
	if err != nil {
		log.Fatalf("Failed to start client; %v", err)
	}
	defer rpcClient.Conn.Close()

	value, err := rpcClient.Read("a")
	if err != nil {
		log.Printf("Failed to call Read on router; %v", err)
		return
	}
	fmt.Printf("Sucess in Read. Got value: %v", value)
}
