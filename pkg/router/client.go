package router

import (
	"fmt"

	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ShardNodeRPCClient struct {
	ClientAPI shardnodepb.ShardNodeClient
	Conn      *grpc.ClientConn
}

type ReplicaRPCClientMap map[int]ShardNodeRPCClient

func StartShardNodeRPCClients(endpoints []config.ShardNodeEndpoint) (map[int]ReplicaRPCClientMap, error) {
	clients := make(map[int]ReplicaRPCClientMap)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		clientAPI := shardnodepb.NewShardNodeClient(conn)
		if len(clients[endpoint.ID]) == 0 {
			clients[endpoint.ID] = make(ReplicaRPCClientMap)
		}
		clients[endpoint.ID][endpoint.ReplicaID] = ShardNodeRPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
}
