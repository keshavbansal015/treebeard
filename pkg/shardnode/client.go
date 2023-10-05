package shardnode

import (
	"context"
	"fmt"
	"math/rand"

	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/rs/zerolog/log"
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
	log.Debug().Msgf("Getting random oram node replica map from RPC client map %v", r)
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

	log.Debug().Msgf("Calling all oram node replicas with block requests %v", blockRequests)
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
	log.Debug().Msgf("Got reply from oram node replicas: %v", oramNodeReply)
	return oramNodeReply, nil
}

func StartOramNodeRPCClients(endpoints []config.OramNodeEndpoint) (map[int]ReplicaRPCClientMap, error) {
	log.Debug().Msgf("Starting OramNode RPC clients for endpoints: %v", endpoints)
	clients := make(map[int]ReplicaRPCClientMap)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		log.Debug().Msgf("Starting OramNode RPC client for endpoint: %s", serverAddr)
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
