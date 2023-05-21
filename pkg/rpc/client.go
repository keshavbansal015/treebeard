package rpc

import (
	"fmt"

	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientFactory interface {
	NewClient(conn *grpc.ClientConn) interface{}
}

type RPCClient struct {
	ClientAPI interface{}
	Conn      *grpc.ClientConn
}

func StartRPCClients(endpoints []config.Endpoint, factory ClientFactory) (map[int]RPCClient, error) {
	clients := make(map[int]RPCClient)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		clientAPI := factory.NewClient(conn)
		clients[endpoint.ID] = RPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
}
