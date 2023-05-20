package main

import (
	"context"
	"fmt"
	"log"

	layeronepb "github.com/dsg-uwaterloo/oblishard/proto/layerone"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startRPCClient() (layeronepb.LayerOneClient, *grpc.ClientConn, error) {
	serverAddr := fmt.Sprintf("localhost:%d", 8765) //TODO change this to use env vars or other dynamic mechanisms
	conn, err := grpc.Dial(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials())) //TODO change to use TLS if needed
	if err != nil {
		return nil, nil, err
	}
	client := layeronepb.NewLayerOneClient(conn)
	return client, conn, nil
}

type client struct {
	clientAPI layeronepb.LayerOneClient
}

func (c client) Read(block string) (value string, err error) {
	reply, err := c.clientAPI.Read(context.Background(),
		&layeronepb.ReadRequest{Block: block})
	if err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (c client) Write(block string, value string) (success bool, err error) {
	reply, err := c.clientAPI.Write(context.Background(),
		&layeronepb.WriteRequest{Block: block, Value: value})
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

	value, err := client.Read("test")
	if err != nil {
		log.Printf("Failed to call Read on grpc server; %v", err)
		return
	}
	fmt.Printf("Sucess in Read. Got value: %v", value)
}
