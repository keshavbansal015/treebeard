package rpc

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func ContextPropagationUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	propagators := otel.GetTextMapPropagator()
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.MD{}
		}
		propagators.Inject(ctx, &metadataSupplier{metadata: &md})
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func ContextPropagationUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	propagators := otel.GetTextMapPropagator()
	return func(
		ctx context.Context,
		req interface{},
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.MD{}
		}
		ctx = metadata.NewOutgoingContext(ctx, md)
		ctx = propagators.Extract(ctx, &metadataSupplier{metadata: &md})
		return handler(ctx, req)
	}
}

func GetContextWithRequestID(ctx context.Context) context.Context {
	requestID := uuid.New().String()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("requestid", requestID))
	return ctx
}

func GetRequestIDFromContext(ctx context.Context) (string, error) {
	tracer := otel.Tracer("")
	ctx, span := tracer.Start(ctx, "get request id from context")
	md, _ := metadata.FromIncomingContext(ctx)
	requestID, exists := md["requestid"]
	if !exists || len(requestID) == 0 {
		return "", fmt.Errorf("requestid not found in the request metadata")
	}
	span.End()
	return requestID[0], nil
}
