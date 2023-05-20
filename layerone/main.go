package main

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/dsg-uwaterloo/oblishard/proto/layerone"
	"google.golang.org/grpc"
)

type layerOneServer struct {
	pb.UnimplementedLayerOneServer
}

func (l1 *layerOneServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	fmt.Println("Read on LayerOne is called")
	return &pb.ReadReply{Value: "test"}, nil
}

func (l1 *layerOneServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	fmt.Println("Write on LayerOne is called")
	return &pb.WriteReply{Success: true}, nil
}

func startRPCServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8765)) //TODO change this to use env vars or other dynamic mechanisms
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterLayerOneServer(grpcServer, &layerOneServer{})
	grpcServer.Serve(lis)
}

func main() {
	startRPCServer()
}
