package router

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"time"

	pb "github.com/dsg-uwaterloo/oblishard/api/router"
	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	utils "github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"google.golang.org/grpc"
)

type routerServer struct {
	pb.UnimplementedRouterServer
	shardNodeRPCClients map[int]ReplicaRPCClientMap
	routerID            int
	hasher              utils.Hasher
}

func (r *routerServer) whereToForward(block string) (shardNodeID int) {
	h := r.hasher.Hash(block)
	return int(math.Mod(float64(h), float64(len(r.shardNodeRPCClients))))
}

func (r *routerServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	whereToForward := r.whereToForward(readRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	type readResult struct {
		reply *shardnodepb.ReadReply
		err   error
	}
	responseChannel := make(chan readResult)
	for _, client := range shardNodeRPCClient {
		go func(client ShardNodeRPCClient) {
			reply, err := client.ClientAPI.Read(ctx, &shardnodepb.ReadRequest{Block: readRequest.Block})
			responseChannel <- readResult{reply: reply, err: err}
		}(client)
	}

	timeout := time.After(2 * time.Second)
	for {
		select {
		case readResult := <-responseChannel:
			if readResult.err == nil {
				return &pb.ReadReply{Value: readResult.reply.Value}, nil
			}
		case <-timeout:
			return nil, fmt.Errorf("could not read value from the shardnode")
		}
	}
}

func (r *routerServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	whereToForward := r.whereToForward(writeRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	type writeResult struct {
		reply *shardnodepb.WriteReply
		err   error
	}
	responseChannel := make(chan writeResult)
	for _, client := range shardNodeRPCClient {
		go func(client ShardNodeRPCClient) {
			reply, err := client.ClientAPI.Write(ctx, &shardnodepb.WriteRequest{Block: writeRequest.Block, Value: writeRequest.Value})
			responseChannel <- writeResult{reply: reply, err: err}
		}(client)
	}

	timeout := time.After(2 * time.Second)
	for {
		select {
		case writeResult := <-responseChannel:
			if writeResult.err == nil {
				return &pb.WriteReply{Success: true}, nil
			}
		case <-timeout:
			return nil, fmt.Errorf("could not read value from the shardnode")
		}
	}
}

func StartRPCServer(shardNodeRPCClients map[int]ReplicaRPCClientMap, routerID int, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	routerServer := &routerServer{
		shardNodeRPCClients: shardNodeRPCClients,
		routerID:            routerID,
		hasher:              utils.Hasher{KnownHashes: make(map[string]uint32)},
	}

	pb.RegisterRouterServer(grpcServer, routerServer)
	grpcServer.Serve(lis)
}
