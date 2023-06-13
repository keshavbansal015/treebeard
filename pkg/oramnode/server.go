package oramnode

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"google.golang.org/grpc"
)

type oramNodeServer struct {
	pb.UnimplementedOramNodeServer
	oramNodeServerID int
	replicaID        int
}

func (o *oramNodeServer) ReadPath(context.Context, *pb.ReadPathRequest) (*pb.ReadPathReply, error) {
	//TODO: implement
	//TODO: it should return an error if the block does not exist
	return &pb.ReadPathReply{Value: "test_val_from_oram_node"}, nil
}

func (o *oramNodeServer) JoinRaftVoter(context.Context, *pb.JoinRaftVoterRequest) (*pb.JoinRaftVoterReply, error) {
	//TODO: implement
	return nil, nil
}

func StartServer(oramNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string) {
	//TODO: init raft node and join on joinAddr
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", rpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	oramNodeServer := &oramNodeServer{
		oramNodeServerID: oramNodeServerID,
		replicaID:        replicaID,
	}
	grpcServer := grpc.NewServer()
	pb.RegisterOramNodeServer(grpcServer, oramNodeServer)
	grpcServer.Serve(lis)
}
