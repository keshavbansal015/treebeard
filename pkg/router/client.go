package router

import (
	"fmt"

	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var shardNodeIdToPort = map[int]int{
	0: 8766,
	1: 8767,
	2: 8768,
}

type RPCClient struct {
	ClientAPI shardnodepb.ShardNodeClient
	Conn      *grpc.ClientConn
}

func StartRPCClients() (clients map[int]RPCClient, err error) {
	clients = make(map[int]RPCClient)
	for id, port := range shardNodeIdToPort {
		serverAddr := fmt.Sprintf("localhost:%d", port)
		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials())) //TODO change to use TLS if needed
		if err != nil {
			return nil, err
		}
		clientAPI := shardnodepb.NewShardNodeClient(conn)
		clients[id] = RPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
}
