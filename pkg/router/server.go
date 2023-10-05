package router

import (
	"context"
	"fmt"
	"net"

	pb "github.com/dsg-uwaterloo/oblishard/api/router"
	"github.com/dsg-uwaterloo/oblishard/pkg/rpc"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

type routerServer struct {
	pb.UnimplementedRouterServer
	routerID     int
	epochManager *epochManager
}

func newRouterServer(routerID int, epochManager *epochManager) routerServer {
	log.Debug().Msgf("Creating new router server with routerID %d", routerID)
	return routerServer{
		routerID:     routerID,
		epochManager: epochManager,
	}
}

func (r *routerServer) Read(ctx context.Context, readRequest *pb.ReadRequest) (*pb.ReadReply, error) {
	log.Debug().Msgf("Received read request for block %s", readRequest.Block)
	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, "router read request")
	responseChannel := r.epochManager.addRequestToCurrentEpoch(&request{ctx: rpc.GetContextWithRequestID(ctx), operationType: Read, block: readRequest.Block})
	response := <-responseChannel
	readResponse := response.(readResponse)
	if readResponse.err != nil {
		return nil, fmt.Errorf("could not read value from the shardnode; %s", readResponse.err)
	}
	log.Debug().Msgf("Returning read response (value: %s) for block %s", readResponse.value, readRequest.Block)
	span.End()
	return &pb.ReadReply{Value: readResponse.value}, nil
}

func (r *routerServer) Write(ctx context.Context, writeRequest *pb.WriteRequest) (*pb.WriteReply, error) {
	log.Debug().Msgf("Received write request for block %s", writeRequest.Block)
	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, "router write request")
	responseChannel := r.epochManager.addRequestToCurrentEpoch(&request{ctx: rpc.GetContextWithRequestID(ctx), operationType: Write, block: writeRequest.Block, value: writeRequest.Value})
	response := <-responseChannel
	writeResponse := response.(writeResponse)
	if writeResponse.err != nil {
		return nil, fmt.Errorf("could not write value to the shardnode; %s", writeResponse.err)
	}
	log.Debug().Msgf("Returning write response (success: %t) for block %s", writeResponse.success, writeRequest.Block)
	span.End()
	return &pb.WriteReply{Success: writeResponse.success}, nil
}

func StartRPCServer(shardNodeRPCClients map[int]ReplicaRPCClientMap, routerID int, port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.ContextPropagationUnaryServerInterceptor()))

	epochManager := newEpochManager(shardNodeRPCClients, 0) //TODO: change duration
	go epochManager.run()
	routerServer := newRouterServer(routerID, epochManager)
	pb.RegisterRouterServer(grpcServer, &routerServer)
	grpcServer.Serve(lis)
}
