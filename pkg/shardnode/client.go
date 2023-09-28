package shardnode

import (
	"context"
	"fmt"
	"math/rand"

	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
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

func (r *ReplicaRPCClientMap) readPathFromAllOramNodeReplicas(ctx context.Context, requests []blockRequest, storageID int) (*oramnodepb.ReadPathReply, error) {
	var replicaFuncs []rpc.CallFunc
	var clients []interface{}
	for _, c := range *r {
		replicaFuncs = append(replicaFuncs,
			func(ctx context.Context, client interface{}, request interface{}, opts ...grpc.CallOption) (interface{}, error) {
				return client.(oramNodeRPCClient).ClientAPI.ReadPath(ctx, request.(*oramnodepb.ReadPathRequest), opts...)
			},
		)
		clients = append(clients, c)
	}

	var blockRequests []*oramnodepb.BlockRequest
	for _, request := range requests {
		blockRequests = append(blockRequests, &oramnodepb.BlockRequest{Block: request.block, Path: int32(request.path)})
	}

	reply, err := rpc.CallAllReplicas(
		ctx,
		clients,
		replicaFuncs,
		&oramnodepb.ReadPathRequest{
			Requests:  blockRequests,
			StorageId: int32(storageID),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not get value from the oramnode; %s", err)
	}
	oramNodeReply := reply.(*oramnodepb.ReadPathReply)
	return oramNodeReply, nil
}

func StartOramNodeRPCClients(endpoints []config.OramNodeEndpoint) (map[int]ReplicaRPCClientMap, error) {
	clients := make(map[int]ReplicaRPCClientMap)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(rpc.ContextPropagationUnaryClientInterceptor()),
		)
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
