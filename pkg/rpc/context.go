package rpc

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func ContextPropagationUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		return handler(ctx, req)
	}
}

func GetContextWithRequestID() context.Context {
	requestID := uuid.New().String()
	ctx := context.Background()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("requestid", requestID))
	return ctx
}

func GetRequestIDFromContext(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	requestID, exists := md["requestid"]
	if !exists || len(requestID) == 0 {
		return "", fmt.Errorf("requestid not found in the request metadata")
	}
	return requestID[0], nil
}
