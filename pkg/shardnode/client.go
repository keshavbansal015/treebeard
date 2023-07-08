package shardnode

import (
	"fmt"
	"math/rand"

	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type oramNodeRPCClient struct {
	ClientAPI oramnodepb.OramNodeClient
	Conn      *grpc.ClientConn
}

type ReplicaRPCClientMap map[int]oramNodeRPCClient

type RPCClientMap map[int]ReplicaRPCClientMap

func (r RPCClientMap) getRandomOramNodeReplicaMap() ReplicaRPCClientMap {
	oramNodesLen := len(r)
	randomOramNodeIndex := rand.Intn(oramNodesLen)
	randomOramNode := r[randomOramNodeIndex]
	return randomOramNode
}

func StartOramNodeRPCClients(endpoints []config.OramNodeEndpoint) (map[int]ReplicaRPCClientMap, error) {
	clients := make(map[int]ReplicaRPCClientMap)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		clientAPI := oramnodepb.NewOramNodeClient(conn)
		if len(clients[endpoint.ID]) == 0 {
			clients[endpoint.ID] = make(ReplicaRPCClientMap)
		}
		clients[endpoint.ID][endpoint.ReplicaID] = oramNodeRPCClient{ClientAPI: clientAPI, Conn: conn}
	}
	return clients, nil
}
