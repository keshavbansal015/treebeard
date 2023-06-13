package shardnode

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	leadernotifpb "github.com/dsg-uwaterloo/oblishard/api/leadernotif"
	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
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
	shardNodeServerID       int
	replicaID               int
	raftNode                *raft.Raft
	shardNodeFSM            *shardNodeFSM
	responseChannel         map[string]chan string //map of requestId to their channel for receiving response
	oramNodeClients         map[int]ReplicaRPCClientMap
	oramNodeLeaderNodeIDMap map[int]int //map of oram node id to the current leader
}

type OperationType int

const (
	Read = iota
	Write
)

// TODO: currently if a shardnode fails and restarts it loses its prior knowledge about the current leader
func (s *shardNodeServer) subscribeToOramNodeLeaderChanges() {
	serverAddr := fmt.Sprintf("%s:%d", "127.0.0.1", 1212) // TODO: change this to a dynamic format
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln("can't dial the leadernotif service")
	}
	clientAPI := leadernotifpb.NewLeaderNotifClient(conn)
	stream, err := clientAPI.Subscribe(
		context.Background(),
		&leadernotifpb.SubscribeRequest{
			NodeLayer: "oramnode",
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
		s.oramNodeLeaderNodeIDMap[nodeID] = int(newLeaderID)
	}
}

func (s *shardNodeServer) handleReadPathFromOramNode(ctx context.Context, block string, requestID string) (responseReplicationCommand []byte, err error) {
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()

	oramNodesLen := len(s.oramNodeClients)
	randomOramNodeIndex := rand.Intn(oramNodesLen)
	randomOramNode := s.oramNodeClients[randomOramNodeIndex]
	leader := randomOramNode[s.oramNodeLeaderNodeIDMap[randomOramNodeIndex]]
	if s.shardNodeFSM.requestLog[block][0] != requestID {
		randomPath := 0      // TODO: change after adding the tree logic
		randomStorageID := 0 //TODO: change after adding the tree logic
		leader.ClientAPI.ReadPath(
			ctx,
			&oramnodepb.ReadPathRequest{
				Block:     block,
				Path:      int32(randomPath),
				StorageId: int32(randomStorageID),
				IsReal:    false,
			},
		)
		// ignore the response and wait for the actual response on the channel
	} else {
		path := 0      //TODO: read from position map
		storageID := 0 //TODO: read from position map
		reply, err := leader.ClientAPI.ReadPath(
			ctx,
			&oramnodepb.ReadPathRequest{
				Block:     block,
				Path:      int32(path),
				StorageId: int32(storageID),
				IsReal:    true,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("could not call the ReadPath RPC on the oram node. %s", err)
		}
		fmt.Printf("the reply value is: %s", reply.Value)
		//repicate the response
		responseReplicationPayload, err := msgpack.Marshal(
			&ReplicateResponsePayload{
				Response: reply.Value,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("could not marshal the response replication payload; %s", err)
		}
		responseReplicationCommand, err := msgpack.Marshal(
			&Command{
				Type:      ReplicateResponseCommand,
				RequestID: requestID,
				Payload:   responseReplicationPayload,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("could not marshal the response replication command; %s", err)
		}
		return responseReplicationCommand, nil
	}
	return nil, nil
}

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

	responseReplicationCommand, err := s.handleReadPathFromOramNode(ctx, block, requestID)
	if err != nil {
		return fmt.Errorf("could not handle read path; %s", err)
	}
	//TODO: make the timeout accurate
	//TODO: should i lock the raftNode?
	err = s.raftNode.Apply(responseReplicationCommand, 1*time.Second).Error()
	if err != nil {
		return fmt.Errorf("could not apply log to the FSM; %s", err)
	}

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

func StartServer(shardNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string, oramNodeRPCClients map[int]ReplicaRPCClientMap) {
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
		shardNodeServerID:       shardNodeServerID,
		replicaID:               replicaID,
		raftNode:                r,
		responseChannel:         make(map[string]chan string),
		shardNodeFSM:            shardNodeFSM,
		oramNodeClients:         oramNodeRPCClients,
		oramNodeLeaderNodeIDMap: make(map[int]int),
	}
	go shardnodeServer.announceLeadershipChanges()
	grpcServer := grpc.NewServer()
	pb.RegisterShardNodeServer(grpcServer, shardnodeServer)
	grpcServer.Serve(lis)
}
