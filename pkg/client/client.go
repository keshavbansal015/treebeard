package client

import (
	"context"
	"fmt"

	routerpb "github.com/dsg-uwaterloo/oblishard/api/router"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCClient struct {
	ClientAPI routerpb.RouterClient
	Conn      *grpc.ClientConn
}

func StartRPCClient() (RPCClient, error) {
	serverAddr := fmt.Sprintf("localhost:%d", 8765) //TODO change this to use env vars or other dynamic mechanisms
	conn, err := grpc.Dial(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials())) //TODO change to use TLS if needed
	if err != nil {
		return RPCClient{}, err
	}
	client := routerpb.NewRouterClient(conn)
	return RPCClient{ClientAPI: client, Conn: conn}, nil
}

func (c *RPCClient) Read(block string) (value string, err error) {
	reply, err := c.ClientAPI.Read(context.Background(),
		&routerpb.ReadRequest{Block: block})
	if err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (c *RPCClient) Write(block string, value string) (success bool, err error) {
	reply, err := c.ClientAPI.Write(context.Background(),
		&routerpb.WriteRequest{Block: block, Value: value})
	if err != nil {
		return false, err
	}
	return reply.Success, nil
}
