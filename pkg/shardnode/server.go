package shardnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"

	pb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TODO: ensure that concurrent accesses to this struct from different gRPC calls don't cause race conditions
type shardNodeServer struct {
	pb.UnimplementedShardNodeServer
	shardNodeServerID int
	replicaID         int
	raftNode          *raft.Raft
}

func (s *shardNodeServer) isLeader() (isLeader bool, leaderID int, err error) {
	_, serverID := s.raftNode.LeaderWithID()
	if serverID == "" {
		return false, 0, fmt.Errorf("the leader is unknown or there is no current leader")
	}
	leaderReplicaID, _ := strconv.Atoi(string(serverID))
	if leaderReplicaID == s.replicaID {
		return true, s.replicaID, nil
	} else {
		return false, leaderReplicaID, nil
	}
}

func (s *shardNodeServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	isLeader, leaderID, err := s.isLeader()
	if err != nil {
		return &pb.ReadReply{Value: "", LeaderNodeId: -1}, err
	}
	if !isLeader {
		return &pb.ReadReply{Value: "", LeaderNodeId: int32(leaderID)}, nil
	}
	fmt.Println("Read on shard node is called")
	return &pb.ReadReply{Value: "test", LeaderNodeId: int32(leaderID)}, nil
}

func (s *shardNodeServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	isLeader, leaderID, err := s.isLeader()
	if err != nil {
		return &pb.WriteReply{Success: false, LeaderNodeId: -1}, err
	}
	if !isLeader {
		return &pb.WriteReply{Success: false, LeaderNodeId: int32(leaderID)}, nil
	}

	fmt.Println("Write on shard node is called")
	return &pb.WriteReply{Success: true, LeaderNodeId: int32(leaderID)}, nil
}

func (s *shardNodeServer) JoinRaftVoter(ctx context.Context, joinRaftVoterRequest *pb.JoinRaftVoterRequest) (*pb.JoinRaftVoterReply, error) {
	requestingNodeId := joinRaftVoterRequest.NodeId
	requestingNodeAddr := joinRaftVoterRequest.NodeAddr

	log.Printf("received join request from node %d at %s", requestingNodeId, requestingNodeAddr)

	err := s.raftNode.AddVoter(
		raft.ServerID(strconv.Itoa(int(requestingNodeId))),
		raft.ServerAddress(requestingNodeAddr),
		0, 0).Error()

	if err != nil {
		return &pb.JoinRaftVoterReply{Success: false}, fmt.Errorf("voter could not be added to the leader; %s", err)
	}
	return &pb.JoinRaftVoterReply{Success: true}, nil
}

func StartServer(shardNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string) {
	isFirst := joinAddr == ""
	r, err := startRaftServer(isFirst, replicaID, raftPort)
	if err != nil {
		log.Fatalf("The raft node creation did not succeed; %s", err)
	}

	if !isFirst {
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("The raft node could not connect to the leader as a new voter; %s", err)
		}
		client := pb.NewShardNodeClient(conn)
		joinRaftVoterReply, err := client.JoinRaftVoter(
			context.Background(),
			&pb.JoinRaftVoterRequest{
				NodeId:   int32(replicaID),
				NodeAddr: fmt.Sprintf("localhost:%d", raftPort),
			},
		)
		if err != nil || !joinRaftVoterReply.Success {
			log.Fatalf("The raft node could not connect to the leader as a new voter; %s", err)
		}
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", rpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterShardNodeServer(grpcServer,
		&shardNodeServer{
			shardNodeServerID: shardNodeServerID,
			replicaID:         replicaID,
			raftNode:          r,
		})
	grpcServer.Serve(lis)
}
