package router

import (
	"fmt"

	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCClient struct {
	ClientAPI shardnodepb.ShardNodeClient
	Conn      *grpc.ClientConn
}

func StartRPCClients(shardNodeEndpoints []config.Endpoint) (clients map[int]RPCClient, err error) {
	clients = make(map[int]RPCClient)
	for _, shardNodeEndpoint := range shardNodeEndpoints {
		serverAddr := fmt.Sprintf("%s:%d", shardNodeEndpoint.IP, shardNodeEndpoint.Port)
		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials())) //TODO change to use TLS if needed
		if err != nil {
			return nil, err
		}
		clientAPI := shardnodepb.NewShardNodeClient(conn)
		clients[shardNodeEndpoint.ID] = RPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
}
