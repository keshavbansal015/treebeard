package shardnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	oramnodepb "github.com/dsg-uwaterloo/oblishard/api/oramnode"
	pb "github.com/dsg-uwaterloo/oblishard/api/shardnode"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type shardNodeServer struct {
	pb.UnimplementedShardNodeServer
	shardNodeServerID int
	replicaID         int
	raftNode          *raft.Raft
	shardNodeFSM      *shardNodeFSM
	oramNodeClients   RPCClientMap
}

func newShardNodeServer(shardNodeServerID int, replicaID int, raftNode *raft.Raft, fsm *shardNodeFSM, oramNodeRPCClients RPCClientMap) *shardNodeServer {
	return &shardNodeServer{
		shardNodeServerID: shardNodeServerID,
		replicaID:         replicaID,
		raftNode:          raftNode,
		shardNodeFSM:      fsm,
		oramNodeClients:   oramNodeRPCClients,
	}
}

type OperationType int

const (
	Read = iota
	Write
)

func (s *shardNodeServer) getPathAndStorageBasedOnRequest(block string, requestID string) (path int, storageID int) {
	if !s.shardNodeFSM.isInitialRequest(block, requestID) {
		return 0, 0 // TODO: change to a random path and storage after adding the tree logic
	} else {
		return 0, 0 // TODO: change to a real path and storage from position map after adding the tree logic
	}
}

func (s *shardNodeServer) createResponseChannelForRequestID(requestID string) chan string {
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()
	ch := make(chan string)
	s.shardNodeFSM.responseChannel[requestID] = ch
	return ch
}

func (s *shardNodeServer) readPathFromAllOramNodeReplicas(ctx context.Context, oramNodeReplicaMap ReplicaRPCClientMap, block string, path int, storageID int, isReal bool) (*oramnodepb.ReadPathReply, error) {
	var replicaFuncs []rpc.CallFunc
	var clients []interface{}
	for _, c := range oramNodeReplicaMap {
		replicaFuncs = append(replicaFuncs,
			func(ctx context.Context, client interface{}, request interface{}, opts ...grpc.CallOption) (interface{}, error) {
				return client.(oramNodeRPCClient).ClientAPI.ReadPath(ctx, request.(*oramnodepb.ReadPathRequest), opts...)
			},
		)
		clients = append(clients, c)
	}

	reply, err := rpc.CallAllReplicas(
		ctx,
		clients,
		replicaFuncs,
		&oramnodepb.ReadPathRequest{
			Block:     block,
			Path:      int32(path),
			StorageId: int32(storageID),
			IsReal:    isReal,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not get value from the oramnode; %s", err)
	}
	oramNodeReply := reply.(*oramnodepb.ReadPathReply)
	return oramNodeReply, nil
}

func (s *shardNodeServer) query(ctx context.Context, op OperationType, block string, value string) (string, error) {
	if s.raftNode.State() != raft.Leader {
		return "", fmt.Errorf("not the leader node")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	requestID := md["requestid"][0]

	requestReplicationCommand, err := newRequestReplicationCommand(block, requestID)
	if err != nil {
		return "", fmt.Errorf("could not create request replication command; %s", err)
	}
	err = s.raftNode.Apply(requestReplicationCommand, 2*time.Second).Error()
	if err != nil {
		return "", fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	path, storageID := s.getPathAndStorageBasedOnRequest(block, requestID)
	oramNodeReplicaMap := s.oramNodeClients.getRandomOramNodeReplicaMap()
	reply, err := s.readPathFromAllOramNodeReplicas(ctx, oramNodeReplicaMap, block, path, storageID, s.shardNodeFSM.isInitialRequest(block, requestID))

	if err != nil {
		return "", fmt.Errorf("could not call the ReadPath RPC on the oram node. %s", err)
	}
	responseChannel := s.createResponseChannelForRequestID(requestID)
	if s.shardNodeFSM.isInitialRequest(block, requestID) {
		responseReplicationCommand, err := newResponseReplicationCommand(reply.Value, requestID, block, value, op)
		if err != nil {
			return "", fmt.Errorf("could not create response replication command; %s", err)
		}
		err = s.raftNode.Apply(responseReplicationCommand, 2*time.Second).Error()
		if err != nil {
			return "", fmt.Errorf("could not apply log to the FSM; %s", err)
		}
	}
	responseValue := <-responseChannel
	return responseValue, nil
}

func (s *shardNodeServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	fmt.Println("Read on shard node is called")
	val, err := s.query(ctx, Read, readRequest.Block, "")
	if err != nil {
		return nil, err
	}
	return &pb.ReadReply{Value: val}, nil
}

func (s *shardNodeServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	fmt.Println("Write on shard node is called")
	val, err := s.query(ctx, Write, writeRequest.Block, writeRequest.Value)
	if err != nil {
		return nil, err
	}
	return &pb.WriteReply{Success: val == writeRequest.Value}, nil
}

func (s *shardNodeServer) getBlocksForSend(maxBlocks int) (blocksToReturn []*pb.Block, blocks []string) {
	s.shardNodeFSM.mu.Lock()
	defer s.shardNodeFSM.mu.Unlock()

	counter := 0
	for block, stashState := range s.shardNodeFSM.stash {
		//TODO: just return blocks that are for the request.path and request.storageID after adding the positionmap

		//Don't send a stash block that is in the waiting status to another SendBlocks request
		if stashState.waitingStatus {
			continue
		}
		if counter == int(maxBlocks) {
			break
		}
		blocksToReturn = append(blocksToReturn, &pb.Block{Block: block, Value: stashState.value})
		blocks = append(blocks, block)
		counter++

	}
	return blocksToReturn, blocks
}

func (s *shardNodeServer) SendBlocks(ctx context.Context, request *pb.SendBlocksRequest) (*pb.SendBlocksReply, error) {

	blocksToReturn, blocks := s.getBlocksForSend(int(request.MaxBlocks))

	sentBlocksReplicationCommand, err := newSentBlocksReplicationCommand(blocks)
	if err != nil {
		return nil, fmt.Errorf("could not create sent blocks replication command; %s", err)
	}
	err = s.raftNode.Apply(sentBlocksReplicationCommand, 2*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}

	return &pb.SendBlocksReply{Blocks: blocksToReturn}, nil
}

func (s *shardNodeServer) AckSentBlocks(ctx context.Context, reply *pb.AckSentBlocksRequest) (*pb.AckSentBlocksReply, error) {
	var ackedBlocks []string
	var nackedBlocks []string
	for _, ack := range reply.Acks {
		block := ack.Block
		is_ack := ack.IsAck
		if is_ack {
			ackedBlocks = append(ackedBlocks, block)
		} else {
			nackedBlocks = append(nackedBlocks, block)
		}
	}

	acksNacksReplicationCommand, err := newAcksNacksReplicationCommand(ackedBlocks, nackedBlocks)
	if err != nil {
		return nil, fmt.Errorf("could not create acks/nacks replication command")
	}
	err = s.raftNode.Apply(acksNacksReplicationCommand, 1*time.Second).Error()
	if err != nil {
		return nil, fmt.Errorf("could not apply log to the FSM; %s", err)
	}
	return &pb.AckSentBlocksReply{Success: true}, nil
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

func StartServer(shardNodeServerID int, rpcPort int, replicaID int, raftPort int, joinAddr string, oramNodeRPCClients map[int]ReplicaRPCClientMap) {
	isFirst := joinAddr == ""
	shardNodeFSM := newShardNodeFSM()
	r, err := startRaftServer(isFirst, replicaID, raftPort, shardNodeFSM)
	if err != nil {
		log.Fatalf("The raft node creation did not succeed; %s", err)
	}
	shardNodeFSM.mu.Lock()
	shardNodeFSM.raftNode = r
	shardNodeFSM.mu.Unlock()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			fmt.Println(shardNodeFSM)
		}
	}()

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
	shardnodeServer := newShardNodeServer(shardNodeServerID, replicaID, r, shardNodeFSM, oramNodeRPCClients)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	pb.RegisterShardNodeServer(grpcServer, shardnodeServer)
	grpcServer.Serve(lis)

}
