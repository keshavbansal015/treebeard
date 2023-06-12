package shardnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	leadernotifpb "github.com/dsg-uwaterloo/oblishard/api/leadernotif"
	pb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// TODO: ensure that concurrent accesses to this struct from different gRPC calls don't cause race conditions
type shardNodeServer struct {
	pb.UnimplementedShardNodeServer
	shardNodeServerID int
	replicaID         int
	raftNode          *raft.Raft
	shardNodeFSM      *shardNodeFSM
	responseChannel   map[string]chan string //map of requestId to their channel for receiving response
}

type OperationType int

const (
	Read = iota
	Write
)

func (s *shardNodeServer) query(ctx context.Context, op OperationType, block string, value string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	requestID := md["requestid"][0]

	requestReplicationPayload, err := msgpack.Marshal(
		&ReplicateRequestAndPathAndStoragePayload{
			RequestedBlock: block,
			Path:           0, //TODO: update to use a real path
			StorageID:      0, //TODO: update to use a real storage id
		},
	)
	if err != nil {
		return fmt.Errorf("could not marshal the request, path, storage replication payload %s", err)
	}
	requestReplicationCommand, err := msgpack.Marshal(
		&Command{
			Type:      ReplicateRequestAndPathAndStorageCommand,
			RequestID: requestID,
			Payload:   requestReplicationPayload,
		},
	)
	if err != nil {
		return fmt.Errorf("could not marshal the request, path, storage replication command %s", err)
	}

	//TODO: make the timeout accurate
	//TODO: should i lock the raftNode?
	err = s.raftNode.Apply(requestReplicationCommand, 1*time.Second).Error()
	if err != nil {
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	func() {
		s.shardNodeFSM.mu.Lock()
		defer s.shardNodeFSM.mu.Unlock()
		if len(s.shardNodeFSM.requestLog[block]) > 1 {
			// new fake random path' and storage'
			// send request to the ORAM node
		} else {
			// send request to the ORAM node
			// replicate after getting the response
		}
	}()

	return nil
}

func (s *shardNodeServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	fmt.Println("Read on shard node is called")
	s.query(ctx, Read, readRequest.Block, "")
	return &pb.ReadReply{Value: "test"}, nil
}

func (s *shardNodeServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	fmt.Println("Write on shard node is called")
	s.query(ctx, Write, writeRequest.Block, writeRequest.Value)
	return &pb.WriteReply{Success: true}, nil
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

func (s *shardNodeServer) announceLeadershipChanges() {
	serverAddr := fmt.Sprintf("%s:%d", "127.0.0.1", 1212) // TODO: change this to a dynamic format
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln("can't dial the leadernotif service")
	}

	clientAPI := leadernotifpb.NewLeaderNotifClient(conn)
	r := s.raftNode
	leaderChangeChan := r.LeaderCh()
	for {
		leaderStatus := <-leaderChangeChan
		if leaderStatus { // if we are the new leader
			clientAPI.Publish(
				context.Background(),
				&leadernotifpb.PublishRequest{
					NodeLayer: "shardnode",
					Id:        int32(s.shardNodeServerID),
					LeaderId:  int32(s.replicaID),
				},
			)
		}
	}
}

func StartServer(shardNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string) {
	isFirst := joinAddr == ""
	shardNodeFSM := newShardNodeFSM()
	r, err := startRaftServer(isFirst, replicaID, raftPort, shardNodeFSM)
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
	shardnodeServer := &shardNodeServer{
		shardNodeServerID: shardNodeServerID,
		replicaID:         replicaID,
		raftNode:          r,
		responseChannel:   make(map[string]chan string),
		shardNodeFSM:      shardNodeFSM,
	}
	go shardnodeServer.announceLeadershipChanges()
	grpcServer := grpc.NewServer()
	pb.RegisterShardNodeServer(grpcServer, shardnodeServer)
	grpcServer.Serve(lis)
}
