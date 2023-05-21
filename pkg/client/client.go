package client

import (
	"context"

	routerpb "github.com/dsg-uwaterloo/oblishard/api/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"google.golang.org/grpc"
)

type RouterClientFactory struct{}

func (f *RouterClientFactory) NewClient(conn *grpc.ClientConn) interface{} {
	return routerpb.NewRouterClient(conn)
}

type RouterRPCClient struct {
	ClientAPI routerpb.RouterClient
	Conn      *grpc.ClientConn
}

func ConvertRPCClientInterfaces(rpcClients map[int]rpc.RPCClient) map[int]RouterRPCClient {
	var routerRPCClients = make(map[int]RouterRPCClient, len(rpcClients))
	for id, rpcClient := range rpcClients {
		routerRPCClients[id] = RouterRPCClient{
			ClientAPI: rpcClient.ClientAPI.(routerpb.RouterClient),
			Conn:      rpcClient.Conn}
	}
	return routerRPCClients
}

func (c *RouterRPCClient) Read(block string) (value string, err error) {
	reply, err := c.ClientAPI.Read(context.Background(),
		&routerpb.ReadRequest{Block: block})
	if err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (c *RouterRPCClient) Write(block string, value string) (success bool, err error) {
	reply, err := c.ClientAPI.Write(context.Background(),
		&routerpb.WriteRequest{Block: block, Value: value})
	if err != nil {
		return false, err
	}
	return reply.Success, nil
}
