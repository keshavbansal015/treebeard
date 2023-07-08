package client

import (
	"fmt"

	routerpb "github.com/dsg-uwaterloo/oblishard/api/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RouterRPCClient struct {
	ClientAPI routerpb.RouterClient
	Conn      *grpc.ClientConn
}

func (c *RouterRPCClient) Read(block string) (value string, err error) {
	reply, err := c.ClientAPI.Read(rpc.GetContextWithRequestID(),
		&routerpb.ReadRequest{Block: block})
	if err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (c *RouterRPCClient) Write(block string, value string) (success bool, err error) {
	reply, err := c.ClientAPI.Write(rpc.GetContextWithRequestID(),
		&routerpb.WriteRequest{Block: block, Value: value})
	if err != nil {
		return false, err
	}
	return reply.Success, nil
}

func StartRouterRPCClients(endpoints []config.RouterEndpoint) (map[int]RouterRPCClient, error) {
	clients := make(map[int]RouterRPCClient)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		clientAPI := routerpb.NewRouterClient(conn)
		clients[endpoint.ID] = RouterRPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
}
