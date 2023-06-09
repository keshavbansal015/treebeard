package router

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"

	leadernotifpb "github.com/dsg-uwaterloo/oblishard/api/leadernotif"
	pb "github.com/dsg-uwaterloo/oblishard/api/router"
	shardnodepb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	utils "github.com/dsg-uwaterloo/oblishard/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type routerServer struct {
	pb.UnimplementedRouterServer
	shardNodeRPCClients      map[int]ReplicaRPCClientMap
	routerID                 int
	shardNodeLeaderNodeIDMap map[int]int //map of shard node id to the current leader
}

func (r *routerServer) whereToForward(block string) (shardNodeID int) {
	h := utils.Hash(block)
	return int(math.Mod(float64(h), float64(len(r.shardNodeRPCClients))))
}

func (r *routerServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	whereToForward := r.whereToForward(readRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	currentLeader := r.shardNodeLeaderNodeIDMap[whereToForward]
	reply, err := shardNodeRPCClient[currentLeader].ClientAPI.Read(ctx,
		&shardnodepb.ReadRequest{Block: readRequest.Block})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return &pb.ReadReply{Value: reply.Value}, nil
}

func (r *routerServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	whereToForward := r.whereToForward(writeRequest.Block)
	shardNodeRPCClient := r.shardNodeRPCClients[whereToForward]

	currentLeader := r.shardNodeLeaderNodeIDMap[whereToForward]
	reply, err := shardNodeRPCClient[currentLeader].ClientAPI.Write(ctx,
		&shardnodepb.WriteRequest{Block: writeRequest.Block, Value: writeRequest.Value})
	if err != nil {
		return &pb.WriteReply{Success: reply.Success}, err
	}

	return &pb.WriteReply{Success: true}, nil
}

// TODO: currently if a router fails and restarts it loses its prior knowledge about the current leader
func (r *routerServer) subscribeToShardNodeLeaderChanges() {
	serverAddr := fmt.Sprintf("%s:%d", "127.0.0.1", 1212) // TODO: change this to a dynamic format
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln("can't dial the leadernotif service")
	}
	clientAPI := leadernotifpb.NewLeaderNotifClient(conn)
	stream, err := clientAPI.Subscribe(
		context.Background(),
		&leadernotifpb.SubscribeRequest{
			NodeLayer: "shardnode",
		},
	)
	if err != nil {
		log.Fatalf("could not connect to the leadernotif service")
	}
	for {
		leaderChange, err := stream.Recv()
		if err != nil {
			log.Fatalf("error in understanding the current leader of the raft cluster")
		}
		newLeaderID := leaderChange.LeaderId
		nodeID := int(leaderChange.Id)
		fmt.Printf("got new leader id for the raft cluster: %d\n", newLeaderID)
		r.shardNodeLeaderNodeIDMap[nodeID] = int(newLeaderID)
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
	}
	routerServer.shardNodeLeaderNodeIDMap = make(map[int]int)
	for i := 0; i < len(routerServer.shardNodeRPCClients); i++ {
		routerServer.shardNodeLeaderNodeIDMap[i] = 0
		// The initial leader is node zero for all shardnodes
		// The routers update this if the leader changes for a shardnode.
		// The routers understand this by subscribing to the leadernotif service
	}
	go routerServer.subscribeToShardNodeLeaderChanges()
	pb.RegisterRouterServer(grpcServer, routerServer)
	grpcServer.Serve(lis)
}
