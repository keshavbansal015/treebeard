package router

import (
	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"google.golang.org/grpc"
)

type ShardNodeClientFactory struct{}

func (f *ShardNodeClientFactory) NewClient(conn *grpc.ClientConn) interface{} {
	return shardnodepb.NewShardNodeClient(conn)
}

type ShardNodeRPCClient struct {
	ClientAPI shardnodepb.ShardNodeClient
	Conn      *grpc.ClientConn
}

func ConvertRPCClientInterfaces(rpcClients map[int]rpc.RPCClient) map[int]ShardNodeRPCClient {
	var shardNodeRPCClients = make(map[int]ShardNodeRPCClient, len(rpcClients))
	for id, rpcClient := range rpcClients {
		shardNodeRPCClients[id] = ShardNodeRPCClient{
			ClientAPI: rpcClient.ClientAPI.(shardnodepb.ShardNodeClient),
			Conn:      rpcClient.Conn}
	}
	return shardNodeRPCClients
}
