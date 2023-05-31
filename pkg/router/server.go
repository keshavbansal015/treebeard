package router

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"

	pb "github.com/dsg-uwaterloo/oblishard/api/router"
	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	utils "github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"google.golang.org/grpc"
)

type routerServer struct {
	pb.UnimplementedRouterServer
	shardNodeRPCClients      map[int]ReplicaRPCClientMap
	routerID                 int
	shardNodeLeaderNodeIDMap map[int]int
}

func (r *routerServer) whereToForward(block string) (shardNodeID int) {
	h := utils.Hash(block)
	return int(math.Mod(float64(h), float64(len(r.shardNodeRPCClients))))
}

// TODO: when the previous leader is down the client should call a random replica node to understand about the new leader
func (r *routerServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	whereToForward := r.whereToForward(readRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	//The client sends the request to the current leader
	currentLeader := r.shardNodeLeaderNodeIDMap[whereToForward]
	reply, err := shardNodeRPCClient[currentLeader].ClientAPI.Read(context.Background(),
		&shardnodepb.ReadRequest{Block: readRequest.Block})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	//The client updates the current leader if the requested node was not a leader.
	if reply.LeaderNodeId != int32(currentLeader) {
		r.shardNodeLeaderNodeIDMap[whereToForward] = int(reply.LeaderNodeId)
		//TODO: a better option would be to retry the request and don't bother the client
		return nil, fmt.Errorf("the leader was not chosen correctly for this request")
	}

	return &pb.ReadReply{Value: reply.Value}, nil
}

func (r *routerServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	whereToForward := r.whereToForward(writeRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	//The client sends the request to the current leader
	currentLeader := r.shardNodeLeaderNodeIDMap[whereToForward]
	reply, err := shardNodeRPCClient[currentLeader].ClientAPI.Write(context.Background(),
		&shardnodepb.WriteRequest{Block: writeRequest.Block, Value: writeRequest.Value})
	if err != nil {
		return &pb.WriteReply{Success: reply.Success}, err
	}
	//The client updates the current leader if the requested node was not a leader.
	if reply.LeaderNodeId != int32(currentLeader) {
		r.shardNodeLeaderNodeIDMap[whereToForward] = int(reply.LeaderNodeId)
		//TODO: a better option would be to retry the request and don't bother the client
		return &pb.WriteReply{Success: reply.Success}, fmt.Errorf("the leader was not chosen correctly for this request")
	}
	return &pb.WriteReply{Success: true}, nil
}

func StartRPCServer(shardNodeRPCClients map[int]ReplicaRPCClientMap, routerID int, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	routerServer := &routerServer{
		shardNodeRPCClients: shardNodeRPCClients,
		routerID:            routerID,
	}
	routerServer.shardNodeLeaderNodeIDMap = make(map[int]int)
	for i := 0; i < len(routerServer.shardNodeRPCClients); i++ {
		routerServer.shardNodeLeaderNodeIDMap[i] = 0
		// The initial leader is node zero for all shardnodes
		// The routers update this if the leader changes for a shardnode
	}
	pb.RegisterRouterServer(grpcServer, routerServer)
	grpcServer.Serve(lis)
}
