package oramnode

import (
	"context"
	"fmt"

	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/config"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ShardNodeRPCClient struct {
	ClientAPI shardnodepb.ShardNodeClient
	Conn      *grpc.ClientConn
}

type ReplicaRPCClientMap map[int]ShardNodeRPCClient

func (r *ReplicaRPCClientMap) sendAcksToShardNode(acks []*shardnodepb.Ack) error {

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

	_, err := rpc.CallAllReplicas(
		context.Background(),
		clients,
		replicaFuncs,
		&shardnodepb.AckSentBlocksRequest{
			Acks: acks,
		},
	)
	if err != nil {
		return fmt.Errorf("could not read blocks from the shardnode; %s", err)
	}
	return nil
}

func (r *ReplicaRPCClientMap) getBlocksFromShardNode(paths []int, storageID int, maxBlocksToSend int) ([]*shardnodepb.Block, error) {

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
			Paths:     utils.ConvertIntSliceToInt32Slice(paths),
			StorageId: int32(storageID),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not read blocks from the shardnode; %s", err)
	}
	shardNodeReply := reply.(*shardnodepb.SendBlocksReply)
	return shardNodeReply.Blocks, nil
}

func (r *ReplicaRPCClientMap) sendBackAcksNacks(recievedBlocksStatus map[string]bool) {
	var acks []*shardnodepb.Ack
	for block, status := range recievedBlocksStatus {
		acks = append(acks, &shardnodepb.Ack{Block: block, IsAck: status})
	}
	r.sendAcksToShardNode(acks)
}

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
