package main

import (
	"context"
	"fmt"
	"log"

	routerpb "github.com/dsg-uwaterloo/oblishard/api/router"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startRPCClient() (routerpb.RouterClient, *grpc.ClientConn, error) {
	serverAddr := fmt.Sprintf("localhost:%d", 8765) //TODO change this to use env vars or other dynamic mechanisms
	conn, err := grpc.Dial(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials())) //TODO change to use TLS if needed
	if err != nil {
		return nil, nil, err
	}
	client := routerpb.NewRouterClient(conn)
	return client, conn, nil
}

type client struct {
	clientAPI routerpb.RouterClient
}

func (c *client) Read(block string) (value string, err error) {
	reply, err := c.clientAPI.Read(context.Background(),
		&routerpb.ReadRequest{Block: block})
	if err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (c *client) Write(block string, value string) (success bool, err error) {
	reply, err := c.clientAPI.Write(context.Background(),
		&routerpb.WriteRequest{Block: block, Value: value})
	if err != nil {
		return false, err
	}
	return reply.Success, nil
}

func main() {
	clientAPI, conn, err := startRPCClient()
	defer conn.Close()
	if err != nil {
		log.Fatalf("Failed to start client; %v", err)
	}
	client := client{clientAPI: clientAPI}

	value, err := client.Read("a")
	if err != nil {
		log.Printf("Failed to call Read on router; %v", err)
		return
	}
	fmt.Printf("Sucess in Read. Got value: %v", value)
}
