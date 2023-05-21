package router

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"

	pb "github.com/dsg-uwaterloo/oblishard/api/router"
	utils "github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"google.golang.org/grpc"
)

type routerServer struct {
	pb.UnimplementedRouterServer
	shardNodeRPCClients map[int]RPCClient //TODO maybe the name layerTwoRPCClients is a bit misleading but I don't know
}

func (r *routerServer) whereToForward(block string) (port int) {
	h := utils.Hash(block)
	return int(math.Mod(float64(h), float64(len(r.shardNodeRPCClients))))
}

func (r *routerServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	whereToForward := r.whereToForward(readRequest.Block)
	fmt.Println("Read on router is called and I should forward to, ", whereToForward)
	return &pb.ReadReply{Value: "test"}, nil
}

func (r *routerServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	whereToForward := r.whereToForward(writeRequest.Block)
	fmt.Println("Write on router is called and I should forward to, ", whereToForward)
	return &pb.WriteReply{Success: true}, nil
}

func StartRPCServer(shardNodeRPCClients map[int]RPCClient) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8765)) //TODO change this to use env vars or other dynamic mechanisms
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterRouterServer(grpcServer,
		&routerServer{shardNodeRPCClients: shardNodeRPCClients})
	grpcServer.Serve(lis)
}
