package router

import (
	"fmt"
	"math"

	shardnodepb "github.com/keshavbansal015/treebeard/api/shardnode"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/rpc"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ShardNodeRPCClient holds the client API and the gRPC connection.
type ShardNodeRPCClient struct {
	ClientAPI shardnodepb.ShardNodeClient
	Conn      *grpc.ClientConn
}

// ReplicaRPCClientMap maps replica IDs to ShardNodeRPCClient instances.
type ReplicaRPCClientMap map[int]ShardNodeRPCClient

// StartShardNodeRPCClients initializes gRPC clients for all given shard node endpoints.
func StartShardNodeRPCClients(endpoints []config.ShardNodeEndpoint) (map[int]ReplicaRPCClientMap, error) {
	log.Debug().Msgf("Starting ShardNode RPC clients for %d endpoints", len(endpoints))
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
			log.Error().Msgf("Failed to connect to ShardNode at %s: %v", serverAddr, err)
			return nil, err
		}
		log.Debug().Msgf("Successfully connected to ShardNode at %s", serverAddr)

		clientAPI := shardnodepb.NewShardNodeClient(conn)
		if clients[endpoint.ID] == 0 {
			log.Debug().Msgf("Creating new client map for ShardNode ID %d", endpoint.ID)
			clients[endpoint.ID] = make(ReplicaRPCClientMap)
		}
		clients[endpoint.ID][endpoint.ReplicaID] = ShardNodeRPCClient{ClientAPI: clientAPI, Conn: conn}
		log.Debug().Msgf("Added client for ShardNode ID %d, Replica ID %d", endpoint.ID, endpoint.ReplicaID)
	}

	log.Debug().Msgf("Finished starting ShardNode RPC clients. Total clients initialized: %d", len(clients))
	return clients, nil
}
