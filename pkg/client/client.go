package client

import (
	"context"
	"fmt"
	"math/rand"

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

type RouterClients map[int]RouterRPCClient

func (r RouterClients) GetRandomRouter() RouterRPCClient {
	routersLen := len(r)
	randomRouterIndex := rand.Intn(routersLen)
	randomRouter := r[randomRouterIndex]
	return randomRouter
}

func (c *RouterRPCClient) Read(ctx context.Context, block string) (value string, err error) {
	reply, err := c.ClientAPI.Read(ctx,
		&routerpb.ReadRequest{Block: block})
	if err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (c *RouterRPCClient) Write(ctx context.Context, block string, value string) (success bool, err error) {
	reply, err := c.ClientAPI.Write(ctx,
		&routerpb.WriteRequest{Block: block, Value: value})
	if err != nil {
		return false, err
	}
	return reply.Success, nil
}

func StartRouterRPCClients(endpoints []config.RouterEndpoint) (RouterClients, error) {
	clients := make(map[int]RouterRPCClient)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(rpc.ContextPropagationUnaryClientInterceptor()),
		)
		if err != nil {
			return nil, err
		}
		clientAPI := routerpb.NewRouterClient(conn)
		clients[endpoint.ID] = RouterRPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
}
