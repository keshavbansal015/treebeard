package router

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"

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

func newRouterServer(shardNodeRPCClients map[int]ReplicaRPCClientMap, routerID int) routerServer {
	return routerServer{
		shardNodeRPCClients: shardNodeRPCClients,
		routerID:            routerID,
		hasher:              utils.Hasher{KnownHashes: make(map[string]uint32)},
	}
}

func (r *routerServer) whereToForward(block string) (shardNodeID int) {
	h := r.hasher.Hash(block)
	return int(math.Mod(float64(h), float64(len(r.shardNodeRPCClients))))
}

func (r *routerServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	whereToForward := r.whereToForward(readRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	var replicaFuncs []rpc.CallFunc
	var clients []interface{}
	for _, c := range shardNodeRPCClient {
		replicaFuncs = append(replicaFuncs,
			func(ctx context.Context, client interface{}, request interface{}, opts ...grpc.CallOption) (interface{}, error) {
				return client.(ShardNodeRPCClient).ClientAPI.Read(ctx, request.(*shardnodepb.ReadRequest), opts...)
			},
		)
		clients = append(clients, c)
	}
	reply, err := rpc.CallAllReplicas(ctx, clients, replicaFuncs, &shardnodepb.ReadRequest{Block: readRequest.Block})
	if err != nil {
		return nil, fmt.Errorf("could not read value from the shardnode; %s", err)
	}
	shardNodeReply := reply.(*shardnodepb.ReadReply)
	return &pb.ReadReply{Value: shardNodeReply.Value}, nil
}

func (r *routerServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	whereToForward := r.whereToForward(writeRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	var replicaFuncs []rpc.CallFunc
	var clients []interface{}
	for _, c := range shardNodeRPCClient {
		replicaFuncs = append(replicaFuncs,
			func(ctx context.Context, client interface{}, request interface{}, opts ...grpc.CallOption) (interface{}, error) {
				return client.(ShardNodeRPCClient).ClientAPI.Write(ctx, request.(*shardnodepb.WriteRequest), opts...)
			},
		)
		clients = append(clients, c)
	}

	reply, err := rpc.CallAllReplicas(ctx, clients, replicaFuncs, &shardnodepb.WriteRequest{Block: writeRequest.Block, Value: writeRequest.Value})
	if err != nil {
		return nil, fmt.Errorf("could not write value to the shardnode; %s", err)
	}
	shardNodeReply := reply.(*shardnodepb.WriteReply)
	return &pb.WriteReply{Success: shardNodeReply.Success}, nil
}

func StartRPCServer(shardNodeRPCClients map[int]ReplicaRPCClientMap, routerID int, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))

	routerServer := newRouterServer(shardNodeRPCClients, routerID)
	pb.RegisterRouterServer(grpcServer, &routerServer)
	grpcServer.Serve(lis)
}
