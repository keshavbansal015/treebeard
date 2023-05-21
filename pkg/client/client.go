package client

import (
	"context"
	"fmt"

	routerpb "github.com/dsg-uwaterloo/oblishard/api/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCClient struct {
	ClientAPI routerpb.RouterClient
	Conn      *grpc.ClientConn
}

func StartRPCClients(routerEndpoints []config.Endpoint) (clients map[int]RPCClient, err error) {
	clients = make(map[int]RPCClient)
	for _, routerEndpoint := range routerEndpoints {
		serverAddr := fmt.Sprintf("%s:%d", routerEndpoint.IP, routerEndpoint.Port)
		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials())) //TODO change to use TLS if needed
		if err != nil {
			return nil, err
		}
		clientAPI := routerpb.NewRouterClient(conn)
		clients[routerEndpoint.ID] = RPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
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
