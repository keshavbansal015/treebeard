package rpc

import (
	"context"

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
