package shardnode

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"google.golang.org/grpc"
)

type shardNodeServer struct {
	pb.UnimplementedShardNodeServer
} //TODO add id to each shardNodeServer

func (s *shardNodeServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	fmt.Println("Read on shard node is called")
	return &pb.ReadReply{Value: "test"}, nil
}

func (s *shardNodeServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	fmt.Println("Write on shard node is called")
	return &pb.WriteReply{Success: true}, nil
}

func StartRPCServer() { //TODO extract into another package to reduce duplication between layer codes
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8766)) //TODO change this to use env vars or other dynamic mechanisms
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterShardNodeServer(grpcServer, &shardNodeServer{})
	grpcServer.Serve(lis)
}
