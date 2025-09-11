package oramnode

import (
	"context"
	"fmt"
	"math"
	"math/rand"

	shardnodepb "github.com/keshavbansal015/treebeard/api/shardnode"
	"github.com/keshavbansal015/treebeard/pkg/config"
	"github.com/keshavbansal015/treebeard/pkg/rpc"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ShardNodeRPCClient struct {
	ClientAPI shardnodepb.ShardNodeClient
	Conn      *grpc.ClientConn
}

type ReplicaRPCClientMap map[int]ShardNodeRPCClient

type ShardNodeRPCClients map[int]ReplicaRPCClientMap

func (c ShardNodeRPCClients) getRandomShardNodeClient() ReplicaRPCClientMap {
	randomIndex := rand.Intn(len(c))
	log.Debug().Msgf("Selecting a random shard node client with index %d", randomIndex)
	return c[randomIndex]
}

func (r *ReplicaRPCClientMap) sendAcksToShardNode(acks []*shardnodepb.Ack) error {
	log.Debug().Msgf("Preparing to send %d acknowledgements to the shard node replica.", len(acks))
	var replicaFuncs []rpc.CallFunc
	var clients []interface{}
	for _, c := range *r {
		replicaFuncs = append(replicaFuncs,
			func(ctx context.Context, client interface{}, request interface{}, opts ...grpc.CallOption) (interface{}, error) {
				return client.(ShardNodeRPCClient).ClientAPI.AckSentBlocks(ctx, request.(*shardnodepb.AckSentBlocksRequest), opts...)
			},
		)
		clients = append(clients, c)
	}
	log.Debug().Msgf("Sending acknowledgements to shard node replica.")
	_, err := rpc.CallAllReplicas(
		context.Background(),
		clients,
		replicaFuncs,
		&shardnodepb.AckSentBlocksRequest{
			Acks: acks,
		},
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send acks to shard node replicas.")
		return fmt.Errorf("could not read blocks from the shardnode; %s", err)
	}
	log.Debug().Msg("Successfully sent acknowledgements to all shard node replicas.")
	return nil
}

func (r *ReplicaRPCClientMap) getBlocksFromShardNode(storageID int, maxBlocksToSend int) ([]*shardnodepb.Block, error) {
	log.Debug().Msgf("Requesting up to %d blocks from shard node replica with storage ID %d.", maxBlocksToSend, storageID)
	var replicaFuncs []rpc.CallFunc
	var clients []interface{}
	for _, c := range *r {
		replicaFuncs = append(replicaFuncs,
			func(ctx context.Context, client interface{}, request interface{}, opts ...grpc.CallOption) (interface{}, error) {
				return client.(ShardNodeRPCClient).ClientAPI.SendBlocks(ctx, request.(*shardnodepb.SendBlocksRequest), opts...)
			},
		)
		clients = append(clients, c)
	}
	reply, err := rpc.CallAllReplicas(
		context.Background(),
		clients,
		replicaFuncs,
		&shardnodepb.SendBlocksRequest{
			MaxBlocks: int32(maxBlocksToSend),
			StorageId: int32(storageID),
		},
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get blocks from shard node replicas.")
		return nil, fmt.Errorf("could not read blocks from the shardnode; %s", err)
	}
	log.Debug().Msgf("Got reply from shard node: %v", reply)
	shardNodeReply := reply.(*shardnodepb.SendBlocksReply)
	log.Debug().Msgf("Received %d blocks from shard node.", len(shardNodeReply.Blocks))
	return shardNodeReply.Blocks, nil
}

func (r *ReplicaRPCClientMap) sendBackAcksNacks(recievedBlocksStatus map[string]bool) {
	log.Debug().Msg("Preparing to send acks and nacks based on received block status.")
	var acks []*shardnodepb.Ack
	for block, status := range recievedBlocksStatus {
		log.Debug().Msgf("Block '%s' status: %t", block, status)
		acks = append(acks, &shardnodepb.Ack{Block: block, IsAck: status})
	}
	r.sendAcksToShardNode(acks)
	log.Debug().Msg("Finished sending acks and nacks.")
}

func StartShardNodeRPCClients(endpoints []config.ShardNodeEndpoint) (map[int]ReplicaRPCClientMap, error) {
	log.Debug().Msgf("Starting ShardNode RPC clients for %d endpoints.", len(endpoints))
	clients := make(map[int]ReplicaRPCClientMap)
	for _, endpoint := range endpoints {
		serverAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)
		log.Debug().Msgf("Dialing gRPC server at address: %s", serverAddr)
		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt64), grpc.MaxCallSendMsgSize(math.MaxInt64)))
		if err != nil {
			log.Error().Err(err).Msgf("Failed to dial gRPC server at %s", serverAddr)
			return nil, err
		}
		log.Debug().Msgf("Successfully dialed gRPC server at %s", serverAddr)
		clientAPI := shardnodepb.NewShardNodeClient(conn)
		if len(clients[endpoint.ID]) == 0 {
			clients[endpoint.ID] = make(ReplicaRPCClientMap)
		}
		clients[endpoint.ID][endpoint.ReplicaID] = ShardNodeRPCClient{ClientAPI: clientAPI, Conn: conn}
		log.Debug().Msgf("Created client for ShardNode ID %d, Replica ID %d", endpoint.ID, endpoint.ReplicaID)
	}
	log.Debug().Msg("All ShardNode RPC clients successfully started.")
	return clients, nil
}
