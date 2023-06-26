package oramnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	pb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/dsg-uwaterloo/oblishard/pkg/storage"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type oramNodeServer struct {
	pb.UnimplementedOramNodeServer
	oramNodeServerID int
	replicaID        int
	raftNode         *raft.Raft
	oramNodeFSM      *oramNodeFSM
}

func newOramNodeServer(oramNodeServerID int, replicaID int, raftNode *raft.Raft, oramNodeFSM *oramNodeFSM) *oramNodeServer {
	return &oramNodeServer{
		oramNodeServerID: oramNodeServerID,
		replicaID:        replicaID,
		raftNode:         raftNode,
		oramNodeFSM:      oramNodeFSM,
	}
}

//TODO: we should answer the request if we are the current leader. Add this after implementing raft for this layer.

func (o *oramNodeServer) ReadPath(ctx context.Context, request *pb.ReadPathRequest) (*pb.ReadPathReply, error) {
	if o.raftNode.State() != raft.Leader {
		return nil, fmt.Errorf("not the leader node")
	}

	md, _ := metadata.FromIncomingContext(ctx)
	requestID := md["requestid"][0]

	var offsetList []int
	for level := 0; level < storage.LevelCount; level++ {
		offset, err := storage.GetBlockOffset(level, int(request.Path), request.Block)
		if err != nil {
			return nil, fmt.Errorf("could not get offset from storage")
		}
		offsetList = append(offsetList, offset)
	}
	replicateOffsetAndBeginReadPathCommand, err := newReplicateOffsetListCommand(requestID, offsetList)
	if err != nil {
		return nil, fmt.Errorf("could not create offsetList replication command; %s", err)
	}
	err = o.raftNode.Apply(replicateOffsetAndBeginReadPathCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	var returnValue string
	for level := 0; level < storage.LevelCount; level++ {
		value, err := storage.ReadBlock(level, int(request.Path), offsetList[level])
		if err == nil {
			returnValue = value
		}
	}

	replicateDeleteOffsetListCommand, err := newReplicateDeleteOffsetListCommand(requestID)
	if err != nil {
		return nil, fmt.Errorf("could not create delete offsetList replication command; %s", err)
	}
	err = o.raftNode.Apply(replicateDeleteOffsetListCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	return &pb.ReadPathReply{Value: returnValue}, nil
}

func (o *oramNodeServer) JoinRaftVoter(ctx context.Context, joinRaftVoterRequest *pb.JoinRaftVoterRequest) (*pb.JoinRaftVoterReply, error) {
	requestingNodeId := joinRaftVoterRequest.NodeId
	requestingNodeAddr := joinRaftVoterRequest.NodeAddr

	log.Printf("received join request from node %d at %s", requestingNodeId, requestingNodeAddr)

	err := o.raftNode.AddVoter(
		raft.ServerID(strconv.Itoa(int(requestingNodeId))),
		raft.ServerAddress(requestingNodeAddr),
		0, 0).Error()

	if err != nil {
		return &pb.JoinRaftVoterReply{Success: false}, fmt.Errorf("voter could not be added to the leader; %s", err)
	}
	return &pb.JoinRaftVoterReply{Success: true}, nil
}

func StartServer(oramNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string) {
	isFirst := joinAddr == ""
	oramNodeFSM := newOramNodeFSM()
	r, err := startRaftServer(isFirst, replicaID, raftPort, oramNodeFSM)
	if err != nil {
		log.Fatalf("The raft node creation did not succeed; %s", err)
	}

	go func() {
		for {
			time.Sleep(5 * time.Second)
			fmt.Println(oramNodeFSM)
		}
	}()

	if !isFirst {
		conn, err := grpc.Dial(joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("The raft node could not connect to the leader as a new voter; %s", err)
		}
		client := pb.NewOramNodeClient(conn)
		fmt.Println(raftPort)
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

	//TODO: init raft node and join on joinAddr
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", rpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	oramNodeServer := newOramNodeServer(oramNodeServerID, replicaID, r, oramNodeFSM)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	pb.RegisterOramNodeServer(grpcServer, oramNodeServer)
	grpcServer.Serve(lis)
}
