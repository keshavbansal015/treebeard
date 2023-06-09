package leadernotif

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/dsg-uwaterloo/oblishard/api/leadernotif"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"google.golang.org/grpc"
)

type leaderChangeMessage struct {
	nodeID      int
	newLeaderID int
}

type topic struct {
	subscribers []chan leaderChangeMessage
	mu          sync.Mutex
}

type leaderNotifServer struct {
	pb.UnimplementedLeaderNotifServer
	topics map[string]*topic
}

func (l *leaderNotifServer) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishReply, error) {
	topic := request.NodeLayer
	nodeID := int(request.Id)
	if (topic != "shardnode") && (topic != "oramnode") {
		log.Print("leader change messages can only be published on either shardnode or oramnode topics")
		return nil, fmt.Errorf("leader change messages can only be published on either shardnode or oramnode topics")
	}
	l.topics[topic].mu.Lock()
	defer l.topics[topic].mu.Unlock()
	for _, c := range l.topics[topic].subscribers {
		c <- leaderChangeMessage{nodeID: nodeID, newLeaderID: int(request.LeaderId)}
	}
	return &pb.PublishReply{Success: true}, nil
}

func (l *leaderNotifServer) Subscribe(request *pb.SubscribeRequest, stream pb.LeaderNotif_SubscribeServer) error {
	topic := request.NodeLayer
	if (topic != "shardnode") && (topic != "oramnode") {
		log.Print("subscription is only possible on either shardnode or oramnode topics")
		return fmt.Errorf("subscription is only possible on either shardnode or oramnode topics")
	}
	subChannel := make(chan leaderChangeMessage)
	l.topics[topic].mu.Lock()
	l.topics[topic].subscribers = append(l.topics[topic].subscribers, subChannel)
	l.topics[topic].mu.Unlock()

	defer func() { // removes the subscriber from the list after client disconnects to prevent server getting blocked on publish operations
		l.topics[topic].mu.Lock()
		for i, channel := range l.topics[topic].subscribers {
			if channel == subChannel {
				l.topics[topic].subscribers = append(l.topics[topic].subscribers[:i], l.topics[topic].subscribers[i+1:]...)
			}
		}
		l.topics[topic].mu.Unlock()
	}()

	for {
		leaderChangeMessage := <-subChannel
		stream.Send(
			&pb.LeaderChange{
				LeaderId: int32(leaderChangeMessage.newLeaderID),
			},
		)
	}
}

func StartRPCServer(port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))
	leaderNotifServer := &leaderNotifServer{
		topics: map[string]*topic{
			"shardnode": {},
			"oramnode":  {},
		},
	}
	pb.RegisterLeaderNotifServer(grpcServer, leaderNotifServer)
	grpcServer.Serve(lis)
}
