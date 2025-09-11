package shardnode

import (
	"context"
	"fmt"
	"math"

	oramnodepb "github.com/keshavbansal015/treebeard/api/oramnode"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/rpc"
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

func (r *ReplicaRPCClientMap) readPathFromAllOramNodeReplicas(ctx context.Context, requests []blockRequest, storageID int) (*oramnodepb.ReadPathReply, error) {
	log.Debug().Msgf("Starting readPathFromAllOramNodeReplicas for storageID %d with %d requests", storageID, len(requests))
	var replicaFuncs []rpc.CallFunc
	var clients []interface{}
	for replicaID, c := range *r {
		log.Debug().Msgf("Adding replica %d to call list", replicaID)
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
	log.Debug().Msgf("Prepared %d block requests to send: %v", len(blockRequests), blockRequests)

	log.Debug().Msgf("Calling all %d oram node replicas with ReadPath request", len(clients))
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
		log.Error().Msgf("Failed to get value from the oramnode: %s", err)
		return nil, fmt.Errorf("could not get value from the oramnode; %s", err)
	}
	oramNodeReply := reply.(*oramnodepb.ReadPathReply)
	log.Debug().Msgf("Successfully received reply from oram node replicas with %d responses", len(oramNodeReply.Responses))
	log.Trace().Msgf("Reply details: %v", oramNodeReply)
	return oramNodeReply, nil
}

func StartOramNodeRPCClients(endpoints []config.OramNodeEndpoint) (map[int]ReplicaRPCClientMap, error) {
	log.Debug().Msgf("Starting OramNode RPC clients for %d endpoints", len(endpoints))
	clients := make(map[int]ReplicaRPCClientMap)
	for _, endpoint := range endpoints {
		log.Debug().Msgf("Processing endpoint ID %d, Replica ID %d at %s:%d", endpoint.ID, endpoint.ReplicaID, endpoint.IP, endpoint.Port)
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		log.Debug().Msgf("Attempting to dial gRPC server at address: %s", serverAddr)
		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(rpc.ContextPropagationUnaryClientInterceptor()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt64), grpc.MaxCallSendMsgSize(math.MaxInt64)),
		)
		if err != nil {
			log.Error().Msgf("Failed to connect to OramNode at %s: %v", serverAddr, err)
			return nil, err
		}
		log.Debug().Msgf("Successfully connected to OramNode at %s", serverAddr)
		clientAPI := oramnodepb.NewOramNodeClient(conn)
		if clients[endpoint.ID] == 0 {
			log.Debug().Msgf("Creating new client map for OramNode ID %d", endpoint.ID)
			clients[endpoint.ID] = make(ReplicaRPCClientMap)
		}
		clients[endpoint.ID][endpoint.ReplicaID] = oramNodeRPCClient{ClientAPI: clientAPI, Conn: conn}
		log.Debug().Msgf("Added client for OramNode ID %d, Replica ID %d", endpoint.ID, endpoint.ReplicaID)
	}
	log.Debug().Msgf("Finished starting OramNode RPC clients. Total clients initialized: %d", len(clients))
	return clients, nil
}
